#!/usr/bin/env python3
"""抽样清单端到端自测：重复上传、补传、换版、失败回滚、名额不足。

用法：python tests/sampling_selftest.py
退出码 0 = 全部通过；1 = 有失败项。
"""
import csv
import io
import json
import os
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

_fd, DB = tempfile.mkstemp(prefix="obs-sampling-", suffix=".db")
os.close(_fd)
os.remove(DB)
os.environ["DB_PATH"] = DB

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(("  ✓ " if cond else "  ✗ ") + name + ("" if cond else f"  {extra}"))


def up(c, name, text):
    return c.post("/api/batches", files={"file": (name, text.encode("utf-8"), "text/csv")})


def mk(c, per=2, seed="seed-A", week="2026-W47", column_id="c1", note=""):
    return c.post("/api/sampling/rounds", json={
        "week": week, "column_id": column_id, "per_station": per,
        "seed": seed, "note": note,
    })


def sel(d):
    return {(x["station"], x["hour_utc"]) for x in d["selected"]}


def main():
    with TestClient(app) as c:
        print("== 1. 初版结构 + 一周数据 ==")
        r = c.post("/api/versions", json={"columns": [
            {"role": "station", "name": "站号"},
            {"role": "hour", "name": "UTC小时"},
            {"name": "气温", "type": "integer"},
        ]})
        check("发布初版 v1", r.status_code == 201, r.text[:300])
        temp_id = {x["name"]: x for x in r.json()["columns"]}["气温"]["column_id"]

        rows = []
        # S01 周一 00:00 ~ 06:00，气温 10..16；S02 只有 3 个有值小时 + 1 个空值
        for h in range(7):
            rows.append(f"S01,2026-11-16T{h:02d}:00,{10 + h}")
        for h in range(3):
            rows.append(f"S02,2026-11-16T{h:02d}:00,{20 + h}")
        rows.append("S02,2026-11-16T05:00,")
        r = up(c, "w47.csv", "站号,UTC小时,气温\n" + "\n".join(rows) + "\n")
        check("周数据上传成功(11行)", r.status_code == 201 and r.json()["row_count"] == 11, r.text[:300])

        print("== 2. 生成清单（每站 2 个，固定种子） ==")
        r = mk(c)
        check("生成清单 #1", r.status_code == 201, r.text[:500])
        d1 = r.json()
        check("清单周标注 = 2026-W47", d1["week_label"] == "2026-W47")
        check("清单记录稳定列标识与名字", d1["column"]["column_id"] == temp_id and d1["column"]["name"] == "气温")
        check("清单记录结构版本 v1", d1["structure_version_no"] == 1)
        check("S01 抽中 2 个", sum(1 for x in d1["selected"] if x["station"] == "S01") == 2, json.dumps(d1["selected"], ensure_ascii=False))
        check("S02 抽中 2 个", sum(1 for x in d1["selected"] if x["station"] == "S02") == 2)
        check("S02 空值小时被排除", all(x["hour_utc"] != "2026-11-16T05:00" for x in d1["selected"]))
        check("每周共 168 小时纳入候选范围",
              d1["week_end"] == "2026-11-23" and d1["week_start"] == "2026-11-16")
        check("S02 有 1 个空值候选", any(s["station"] == "S02" and s["empty_count"] == 1 for s in d1["stations"]))
        check("无缺额", all(s["shortfall"] == 0 for s in d1["stations"]))
        sel1 = sel(d1)
        check("选中读数与原始读数一致",
              all(x["selected_value"] is not None for x in d1["selected"]))
        check("选中记录原始批次号", all(x["selected_batch_id"] == 1 for x in d1["selected"]))

        # 独立性核验：按文档算法从清单候选复算，选中集合应完全一致。
        from app.sampling import _order_key
        for st in ("S01", "S02"):
            cand = [x for x in d1["candidates"]
                    if x["station"] == st and x["status"] == "eligible"]
            expect = {x["hour_utc"] for x in sorted(
                cand, key=lambda x: _order_key(d1["seed"], d1["week_label"],
                                               d1["column"]["column_id"],
                                               x["station"], x["hour_utc"]))[:2]}
            check(f"{st} 选中集合可用种子复算", expect ==
                  {h for (s, h) in sel1 if s == st}, f"{expect}")

        print("== 3. 已领用样本跨轮次锁定 ==")
        r = mk(c, per=2, seed="seed-A")
        check("生成清单 #2", r.status_code == 201, r.text[:500])
        d2 = r.json()
        d1b = c.get(f"/api/sampling/rounds/{d1['id']}").json()
        check("清单 #1 = 已发布的完整快照", d1b["selected"] == d1["selected"] and
              d1b["candidates"] == d1["candidates"])
        check("#2 不再领用前轮选中小时", sel(d2).isdisjoint(sel1),
              f"{sel(d2)} ∩ {sel1}")
        s01_2 = next(s for s in d2["stations"] if s["station"] == "S01")
        check("#2 S01 仍有 5 个合格小时可选", s01_2["eligible_count"] == 5,
              json.dumps(s01_2, ensure_ascii=False))
        s02_2 = next(s for s in d2["stations"] if s["station"] == "S02")
        check("#2 S02 合格仅剩 1 个，缺额 1 并说明原因",
              s02_2["eligible_count"] == 1 and s02_2["shortfall"] == 1 and
              "名额不足" in s02_2["reason"], json.dumps(s02_2, ensure_ascii=False))
        check("#2 候选里前轮选中小时状态为 claimed", all(
            any(x["station"] == s and x["hour_utc"] == h and x["status"] == "claimed"
                for x in d2["candidates"])
            for (s, h) in sel1))

        print("== 4. 重复上传：同小时同值只占一个候选位置 ==")
        r = up(c, "dup_same.csv", "站号,UTC小时,气温\nS01,2026-11-16T00:00,10\n")
        check("重复上传成功", r.status_code == 201, r.text[:300])
        d_now = c.get("/api/sampling/rounds/" + str(d1["id"])).json()
        check("旧清单不被补传改动", d_now["selected"] == d1["selected"] and
              d_now["candidates"] == d1["candidates"])
        # S01 有 7 个不同整点，重复上传同一 (S01,00:00) 不应变成 8 个候选。
        r = mk(c, per=20, seed="seed-dup")
        check("同值重复上传不增加候选位置", r.status_code == 201, r.text[:400])
        d_dup = r.json()
        check("S01 候选位置仍为 7 个",
              len([x for x in d_dup["candidates"] if x["station"] == "S01"]) == 7,
              len([x for x in d_dup["candidates"] if x["station"] == "S01"]))
        check("S01/00:00 不是待核", not any(
            x["station"] == "S01" and x["hour_utc"] == "2026-11-16T00:00"
            for x in d_dup["conflicts"]))
        check("S01/00:00 候选记录保留两次同值上传",
              any(x["station"] == "S01" and x["hour_utc"] == "2026-11-16T00:00" and
                  len(x["readings"]) == 2 for x in d_dup["candidates"]))

        print("== 5. 补传异值 → 新轮次列为待核并排除 ==")
        r = up(c, "dup_diff.csv", "站号,UTC小时,气温\nS02,2026-11-16T01:00,999\n")
        check("异值补传成功", r.status_code == 201, r.text[:300])
        r = mk(c, per=2, seed="seed-B")
        check("生成清单 #3", r.status_code == 201, r.text[:500])
        d3 = r.json()
        conf = {(x["station"], x["hour_utc"]) for x in d3["conflicts"]}
        check("S02/01:00 列为待核", ("S02", "2026-11-16T01:00") in conf,
              json.dumps(d3["conflicts"], ensure_ascii=False)[:500])
        check("待核小时不参与抽样", ("S02", "2026-11-16T01:00") not in sel(d3))
        check("待核记录保留全部异值", any(
            len(x["readings"]) >= 2 and {rd["value"] for rd in x["readings"]} >= {21, 999}
            for x in d3["conflicts"] if x["hour_utc"] == "2026-11-16T01:00"))
        check("旧清单 #1 仍保留原读数快照",
              c.get(f"/api/sampling/rounds/{d1['id']}").json()["candidates"] == d1["candidates"])

        print("== 6. 换版（改名+扩型）后新轮次按新范围抽取 ==")
        r = c.post("/api/versions", json={"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "温度", "type": "float"},
        ]})
        check("发布 v2（改名+扩型）", r.status_code == 201, r.text[:300])
        r = up(c, "w47_v2.csv", "station_code,UTC小时,温度\nS03,2026-11-16T10:00,5.5\n")
        check("按 v2 上传新站 S03", r.status_code == 201, r.text[:300])
        r = mk(c, per=2, seed="seed-C")
        check("换版后清单 #4 生成", r.status_code == 201, r.text[:500])
        d4 = r.json()
        check("清单 #4 记录新结构版本 v2", d4["structure_version_no"] == 2)
        check("清单 #4 列名随现役版本更新", d4["column"]["name"] == "温度")
        check("S03 抽中 1 个", sum(1 for x in d4["selected"] if x["station"] == "S03") == 1)
        check("跨轮次不重复领用",
              sel(d4).isdisjoint(sel1) and sel(d4).isdisjoint(sel(d3)))
        check("旧清单 #1 列名仍是发布时快照「气温」",
              c.get(f"/api/sampling/rounds/{d1['id']}").json()["column"]["name"] == "气温")

        print("== 7. 名额不足 → 明确缺额 ==")
        # S02 只剩 3 个候选小时：1 个空值、1 个待核、2 个已被 #1 领用 → 0 可抽
        r = mk(c, per=5, seed="seed-D")
        check("生成清单 #5", r.status_code == 201, r.text[:500])
        d5 = r.json()
        s02 = next(s for s in d5["stations"] if s["station"] == "S02")
        check("S02 标出缺额=5", s02["shortfall"] == 5 and s02["selected_count"] == 0,
              json.dumps(s02, ensure_ascii=False))
        check("S02 缺额说明含原因", "名额不足" in s02["reason"], s02["reason"])
        check("S02 明细统计齐全",
              s02["claimed_count"] == 2 and s02["conflict_count"] == 1 and s02["empty_count"] == 1,
              json.dumps(s02, ensure_ascii=False))
        check("无数据周返回明确错误", mk(c, week="2027-W01").status_code == 400)
        r = c.post("/api/sampling/rounds", json={
            "week": "2026-W48", "column_id": temp_id, "per_station": 2,
            "seed": "seed-empty", "stations": ["S01"]})
        check("显式点名无数据站也可出清单", r.status_code == 201, r.text[:400])
        d_empty = r.json()
        s_empty = d_empty["stations"][0]
        check("无数据站全额缺额并说明原因",
              s_empty["shortfall"] == 2 and s_empty["selected_count"] == 0 and
              not s_empty["has_data"] and "无任何观测数据" in s_empty["reason"],
              json.dumps(s_empty, ensure_ascii=False))
        check("无数据站不产生候选与领用", d_empty["candidates"] == [] and
              not any(x["round_id"] == d_empty["id"]
                      for x in c.get("/api/sampling/meta").json()["claims"]))

        print("== 8. 补传与已领用样本冲突时明确警示（不改旧清单读数） ==")
        # 改写某个 S02 已领用小时（清单 #1 已交读数）为 777：旧清单仍是原值，新轮次须警示。
        claimed_hour = next(h for (s, h) in sel1 if s == "S02")
        claimed_value = next(x["selected_value"] for x in d1["selected"]
                             if x["station"] == "S02" and x["hour_utc"] == claimed_hour)
        r = up(c, "overwrite_claimed.csv",
               f"station_code,UTC小时,温度\nS02,{claimed_hour},777\n")
        check("对已领用小时补传异值成功", r.status_code == 201, r.text[:300])
        r = mk(c, per=1, seed="seed-E")
        check("生成清单 #6", r.status_code == 201, r.text[:500])
        d6 = r.json()
        hit = next(x for x in d6["candidates"]
                   if x["station"] == "S02" and x["hour_utc"] == claimed_hour)
        check("#6 该站时状态仍为 claimed（不重复领用）", hit["status"] in ("claimed", "conflict"),
              json.dumps(hit, ensure_ascii=False))
        check("#6 明确警示已交读数被改动", bool(hit["warnings"]),
              json.dumps(hit, ensure_ascii=False))
        check("#6 警示包含已交读数与当前读数",
              any(str(claimed_value) in w and "777" in w for w in hit["warnings"]),
              json.dumps(hit["warnings"], ensure_ascii=False))
        check("旧清单 #1 已交读数未被悄悄替换",
              next(x for x in c.get(f"/api/sampling/rounds/{d1['id']}").json()["selected"]
                   if x["station"] == "S02" and x["hour_utc"] == claimed_hour)["selected_value"] == claimed_value)

        print("== 9. 失败不占用名额、不改动旧清单 ==")
        before_claims = len(c.get("/api/sampling/meta").json()["claims"])
        before_rounds = len(c.get("/api/sampling/rounds", params={"week": "2026-W47"})
                            .json()["rounds"])
        before_1 = c.get(f"/api/sampling/rounds/{d1['id']}").json()
        bad = c.post("/api/sampling/rounds", json={"week": "2026-W47", "column_id": "station",
                                                   "per_station": 2})
        check("拒绝系统列抽样", bad.status_code == 400, bad.text[:300])
        bad = c.post("/api/sampling/rounds", json={"week": "2026-W47", "column_id": "c99",
                                                   "per_station": 2})
        check("拒绝不存在的列标识", bad.status_code == 400, bad.text[:300])
        bad = c.post("/api/sampling/rounds", json={"week": "2026-W47", "column_id": temp_id,
                                                   "per_station": 0})
        check("拒绝非法每站数量", bad.status_code == 400, bad.text[:300])
        bad = c.post("/api/sampling/rounds", json={"week": "不是周", "column_id": temp_id,
                                                   "per_station": 2})
        check("拒绝非法周", bad.status_code == 400, bad.text[:300])
        after_claims = len(c.get("/api/sampling/meta").json()["claims"])
        after_rounds = len(c.get("/api/sampling/rounds", params={"week": "2026-W47"})
                           .json()["rounds"])
        check("失败未占用名额", before_claims == after_claims, f"{before_claims}→{after_claims}")
        check("失败未产生清单", before_rounds == after_rounds, f"{before_rounds}→{after_rounds}")
        check("失败未改动旧清单", before_1 == c.get(f"/api/sampling/rounds/{d1['id']}").json())

        print("== 10. 清单下载 ==")
        r = c.get(f"/api/sampling/rounds/{d1['id']}/csv")
        check("CSV 下载 200", r.status_code == 200, r.text[:200])
        check("CSV 带附件文件名", "attachment" in r.headers.get("content-disposition", ""))
        rows_csv = list(csv.DictReader(io.StringIO(r.text.lstrip("﻿"))))
        check("CSV 行数 = 候选数", len(rows_csv) == len(d1["candidates"]), f"{len(rows_csv)} vs {len(d1['candidates'])}")
        check("CSV 抽中标记正确", sum(1 for x in rows_csv if x["抽中"] == "是") == len(d1["selected"]))
        check("CSV 记录原始批次与结构版本",
              all(x["选中批次"] and x["选中批次结构版本"] for x in rows_csv if x["抽中"] == "是"))

        print("== 11. 页面与列表 ==")
        r = c.get("/")
        check("首页含抽样清单区块", r.status_code == 200 and "抽样清单" in r.text)
        r = c.get("/api/sampling/rounds")
        check("清单列表按周过滤", len(c.get("/api/sampling/rounds", params={"week": "2026-W47"})
                                       .json()["rounds"]) == before_rounds)
        r = c.get("/api/sampling/rounds/99999")
        check("不存在清单返回 404", r.status_code == 404)
        meta = c.get("/api/sampling/meta").json()
        check("已领用样本可查", len(meta["claims"]) == after_claims and
              all(k in meta["claims"][0] for k in ("station", "hour_utc", "round_id")))

        print("== 12. 数值等价（整数 10 与小数 10.0 视为同一读数） ==")
        r = up(c, "same_numeric.csv",
               "station_code,UTC小时,温度\nS01,2026-11-16T00:00,10.0\n")
        check("扩型后同值补传成功", r.status_code == 201, r.text[:300])
        r = mk(c, per=1, seed="seed-F")
        check("生成清单 #7", r.status_code == 201, r.text[:500])
        d7 = r.json()
        hit = next(x for x in d7["candidates"]
                   if x["station"] == "S01" and x["hour_utc"] == "2026-11-16T00:00")
        check("10 与 10.0 不判为待核", hit["status"] != "conflict",
              json.dumps(hit, ensure_ascii=False))
        check("10 与 10.0 不产生已交样本警示", not hit["warnings"],
              json.dumps(hit["warnings"], ensure_ascii=False))

        print("== 13. 周写法等价 ==")
        from app.sampling import parse_week
        forms = ["2026-W47", "2026-w47", "2026-11-16", "2026-11-19T05:00",
                 "2026-11-22T23:00Z", "2026-W47-7"]
        weeks = [parse_week(f) for f in forms]
        check("多种周/日期写法归一到同一 UTC 周",
              all(w["label"] == "2026-W47" and w["week_start"] == "2026-11-16" and
                  w["week_end"] == "2026-11-23" and len(w["hours"]) == 168 for w in weeks),
              [w["label"] for w in weeks])
        check("周边界小时正确", weeks[0]["hours"][0] == "2026-11-16T00:00" and
              weeks[0]["hours"][-1] == "2026-11-22T23:00")

        print("== 14. 写入中途失败：整体回滚，不占名额 ==")
        import app.sampling as sampling_mod
        from app.db import session as db_session
        before = (len(c.get("/api/sampling/meta").json()["claims"]),
                  len(c.get("/api/sampling/rounds").json()["rounds"]),
                  c.get(f"/api/sampling/rounds/{d1['id']}").json())
        orig = sampling_mod.get_round

        def boom(*_a, **_kw):
            raise RuntimeError("模拟清单写入后、提交前的故障")

        sampling_mod.get_round = boom
        try:
            try:
                with db_session() as conn:
                    sampling_mod.create_round(conn, {
                        "week": "2026-W47", "column_id": temp_id,
                        "per_station": 1, "seed": "seed-rollback"})
                raised = False
            except RuntimeError:
                raised = True
        finally:
            sampling_mod.get_round = orig
        check("故障向上抛出（事务回滚）", raised)
        after = (len(c.get("/api/sampling/meta").json()["claims"]),
                 len(c.get("/api/sampling/rounds").json()["rounds"]),
                 c.get(f"/api/sampling/rounds/{d1['id']}").json())
        check("回滚后未残留领用名额", before[0] == after[0], f"{before[0]}→{after[0]}")
        check("回滚后未残留清单", before[1] == after[1], f"{before[1]}→{after[1]}")
        check("回滚后旧清单未变", before[2] == after[2])
        r = mk(c, per=1, seed="seed-rollback")
        check("同一请求重试成功（名额未被占用）", r.status_code == 201, r.text[:500])

        print("== 15. 跨列领用不误报“已交样本被改动” ==")
        # 清单 #1 领用 S01 的小时用的是 c1（气温）；新增 c2（气压）后按 c2 抽样时，
        # 不得拿气温值去和气压候选比较而误报“已交样本被改动”。
        r = c.post("/api/versions", json={"columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": temp_id, "name": "温度", "type": "float"},
            {"source": None, "name": "气压", "type": "integer", "nullable": True},
        ]})
        check("发布 v3（新增可空列 气压）", r.status_code == 201, r.text[:300])
        pres_id = {x["name"]: x for x in r.json()["columns"]}["气压"]["column_id"]
        rows = "station_code,UTC小时,温度,气压\n" + "\n".join(
            f"S01,2026-11-16T{h:02d}:00,{10 + h},{1000 + h}" for h in range(7))
        r = up(c, "week_pres.csv", rows + "\n")
        check("按 v3 补传气压", r.status_code == 201, r.text[:300])
        r = mk(c, per=1, seed="seed-G", column_id=pres_id)
        check("按另一列生成清单", r.status_code == 201, r.text[:400])
        d_other = r.json()
        shared = next((x for x in d_other["candidates"]
                       if (x["station"], x["hour_utc"]) in sel1), None)
        check("跨列已领用站时仍标记 claimed", shared is not None and
              shared["status"] == "claimed", json.dumps(shared, ensure_ascii=False))
        check("跨列已领用不产生假警示", shared is not None and not shared["warnings"],
              json.dumps(shared and shared["warnings"], ensure_ascii=False))

    print()
    print(f"通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
    if FAIL:
        print("失败项：")
        for n in FAIL:
            print(" -", n)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
