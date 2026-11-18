"""抽样清单核心逻辑（与批次 / 结构版本共用同一 SQLite 事务）。

规则摘要：
- 按 UTC 周 + 站号抽样；周范围是 [周一 00:00, 下周一 00:00)（UTC）。
- 每站每周抽取 N 个「有值」小时；有值 = 该站该小时至少有一条非空读数。
- 同一站同一小时重复上传只占一个候选位置：
  * 各批次的非空读数完全一致时视为同一候选，选第一条非空读数作为入库读数
    （批次号、结构版本一并记录）；
  * 非空读数存在差异时列为 conflict（待核），不参与抽取，并保留全部异值明细。
- 抽样用可复现种子 + sha256 排序键，从合格小时中等概率抽取。
- 前轮清单领用过的 (站号, 小时) 在后续轮次不再参与抽取；名额不足时记录缺额与原因。
- 候选、选中读数、领用记录与清单在同一事务内写入；失败整体回滚，
  不占用名额，也不改动旧清单。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import secrets
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone

from .core import (
    DomainError,
    NotFoundError,
    _now,
    get_active_version,
    normalize_hour,
    version_columns,
)

ALGORITHM = "sha256-order-v1"
MAX_PER_STATION = 168
SEED_MAX_LEN = 200

_WEEK_RE = re.compile(r"^(\d{4})-?W(\d{1,2})(?:-?([1-7]))?$", re.IGNORECASE)
_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")


# ---------------------------------------------------------------- 周范围

def _monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def parse_week(raw) -> dict:
    """接受 2026-W47 / 2026-W47-2 / 2026-11-16 / 2026-11-16T05:00 / ...Z。

    一律归到该天所属 ISO 周（周一 00:00 UTC 起，共 168 小时）。
    返回 {week_start, week_end, label, hours}。
    """
    s = (raw or "").strip()
    if not s:
        raise DomainError("请提供 UTC 周（如 2026-W47 或 2026-11-16）")
    try:
        m = _WEEK_RE.match(s)
        if m:
            y, w, dow = m.group(1), int(m.group(2)), int(m.group(3) or 1)
            d = date.fromisocalendar(int(y), w, dow)
            monday = _monday(d)
        elif _DATE_RE.match(s):
            monday = _monday(date.fromisoformat(s))
        else:
            d = date.fromisoformat(normalize_hour(s)[:10])
            monday = _monday(d)
    except ValueError as e:
        raise DomainError(f"无法识别的 UTC 周 / 日期 {s!r}：{e}")
    week_end = monday + timedelta(days=7)
    iso = monday.isocalendar()
    label = f"{iso[0]}-W{iso[1]:02d}"
    hours = [(datetime(monday.year, monday.month, monday.day)
              + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(168)]
    return {
        "week_start": monday.isoformat(),
        "week_end": week_end.isoformat(),
        "label": label,
        "hours": hours,
    }


def current_week() -> dict:
    """按 UTC 判定“本周”，避免服务器本地时区导致的周归属偏差。"""
    now = datetime.now(timezone.utc)
    return parse_week(now.date().isoformat())


# ---------------------------------------------------------------- 读取辅助

def _round_header(r: dict) -> dict:
    return {
        "id": r["id"],
        "week_label": r["week_label"],
        "week_start": r["week_start"],
        "week_end": r["week_end"],
        "column": {"column_id": r["column_id"], "name": r["column_name"],
                   "type": r["column_type"]},
        "structure_version_no": r["version_no"],
        "per_station": r["per_station"],
        "seed": r["seed"],
        "seed_source": r["seed_source"],
        "algorithm": r["algorithm"],
        "note": r["note"],
        "created_at": r["created_at"],
    }


def get_round_row(conn, round_id: int) -> dict | None:
    r = conn.execute("SELECT * FROM sampling_rounds WHERE id=?", (round_id,)).fetchone()
    return dict(r) if r else None


def _value_for_column(conn, column_id: str) -> dict:
    """在“当前已提交状态”里找到该稳定列标识对应的现役定义。"""
    active = get_active_version(conn)
    if active is None:
        raise DomainError("尚未发布任何结构版本，请先发布初版结构")
    cols = version_columns(conn, active["id"])
    col = next((c for c in cols if c["column_id"] == column_id), None)
    if col is None:
        raise DomainError(f"现役结构（v{active['version_no']}）中没有列标识 {column_id}")
    if col["type"] in ("station", "hour"):
        raise DomainError(f"列 {column_id} 是系统列，请选择数据列作为抽样读数")
    return {"version": active, "column": col}


def list_rounds(conn, week_label: str | None = None) -> list[dict]:
    if week_label:
        rows = conn.execute(
            "SELECT * FROM sampling_rounds WHERE week_label=? ORDER BY id DESC",
            (week_label,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM sampling_rounds ORDER BY id DESC").fetchall()
    out = []
    for r in rows:
        d = _round_header(dict(r))
        sums = conn.execute(
            "SELECT COUNT(*) AS stations, "
            "COALESCE(SUM(selected_count),0) AS selected, "
            "COALESCE(SUM(shortfall),0) AS shortfall "
            "FROM sampling_station_summaries WHERE round_id=?",
            (r["id"],),
        ).fetchone()
        d.update(station_count=sums["stations"], selected_count=sums["selected"],
                 shortfall=sums["shortfall"])
        out.append(d)
    return out


def list_claims(conn) -> list[dict]:
    rows = conn.execute(
        "SELECT station, hour_utc, round_id, column_id, value_json, batch_id, "
        "batch_version_no, created_at FROM sampling_claims "
        "ORDER BY station, hour_utc, round_id"
    ).fetchall()
    return [{**dict(r), "value": json.loads(r["value_json"])} for r in rows]


def _station_summaries(conn, round_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM sampling_station_summaries WHERE round_id=? ORDER BY station",
        (round_id,),
    ).fetchall()
    return [{
        "station": r["station"],
        "requested": r["requested"],
        "selected_count": r["selected_count"],
        "shortfall": r["shortfall"],
        "eligible_count": r["eligible_count"],
        "conflict_count": r["conflict_count"],
        "claimed_count": r["claimed_count"],
        "empty_count": r["empty_count"],
        "warning_count": r["warning_count"],
        "has_data": bool(r["has_data"]),
        "reason": r["reason"],
    } for r in rows]


def _candidates(conn, round_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM sampling_candidates WHERE round_id=? ORDER BY station, hour_utc",
        (round_id,),
    ).fetchall()
    out = []
    for r in rows:
        detail = json.loads(r["detail_json"])
        out.append({
            "station": r["station"],
            "hour_utc": r["hour_utc"],
            "status": r["status"],
            "value": json.loads(r["value_json"]) if r["value_json"] is not None else None,
            "readings": detail["readings"],
            "reason": detail.get("reason", ""),
            "warnings": detail.get("warnings", []),
            "claimed_by_round_id": detail.get("claimed_by_round_id"),
            "claimed_value": detail.get("claimed_value"),
            "selected": bool(r["selected"]),
            "selected_value": (json.loads(r["selected_value_json"])
                               if r["selected_value_json"] is not None else None),
            "selected_batch_id": r["selected_batch_id"],
            "selected_batch_version_no": r["selected_batch_version_no"],
        })
    return out


def get_round(conn, round_id: int) -> dict:
    row = get_round_row(conn, round_id)
    if row is None:
        raise NotFoundError(f"抽样清单 #{round_id} 不存在")
    out = _round_header(row)
    out["stations"] = _station_summaries(conn, round_id)
    candidates = _candidates(conn, round_id)
    out["candidates"] = candidates
    out["selected"] = [c for c in candidates if c["selected"]]
    out["conflicts"] = [c for c in candidates if c["status"] == "conflict"]
    return out


# ---------------------------------------------------------------- 抽样

def _seed_for(raw_seed) -> tuple[str, str]:
    if raw_seed is not None and str(raw_seed).strip() != "":
        seed = str(raw_seed).strip()
        if len(seed) > SEED_MAX_LEN:
            raise DomainError(f"种子过长（上限 {SEED_MAX_LEN} 字符）")
        return seed, "provided"
    # 缺省自动生成一个可重复使用的随机种子；返回值会写入清单，日后可用它精确复算。
    return secrets.token_hex(16), "generated"


def _order_key(seed: str, week_label: str, column_id: str, station: str, hour: str) -> str:
    msg = "\x1f".join((seed, week_label, column_id, station, hour)).encode("utf-8")
    return hashlib.sha256(msg).hexdigest()


def _canon(v) -> str:
    """读数比较键：数值按十进制值比较（5 与 5.0 视为同一读数），文本按原样比较。"""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "b:" + json.dumps(v)
    if isinstance(v, (int, float)):
        return "n:" + format(Decimal(str(v)).normalize(), "f")
    return "t:" + json.dumps(v, ensure_ascii=False, sort_keys=True)


def _dump(v) -> str:
    """入库序列化（保留上传时的原始数值形态：5 仍存为 5，不写成 5.0）。"""
    return json.dumps(v, ensure_ascii=False, sort_keys=True)


def _load_week_groups(conn, week: dict) -> dict:
    """读取该周内全部已提交观测，按 (station, hour) 归并。

    所有合法小时都是整点（YYYY-MM-DDTHH:00），因此直接把本周 168 个规范小时
    作为 IN 条件，避免站位/时区歧义；只读已提交数据，调用方在同一个
    session 事务内完成后续写入，候选范围、已领样本与清单来自同一个已提交状态。
    """
    week_start = week["week_start"] + "T00:00"
    week_end = week["week_end"] + "T00:00"
    rows = conn.execute(
        "SELECT o.station, o.hour_utc, o.values_json, o.batch_id, v.version_no "
        "FROM observations o "
        "JOIN batches b ON b.id = o.batch_id "
        "JOIN schema_versions v ON v.id = b.version_id "
        "WHERE o.hour_utc >= ? AND o.hour_utc < ? "
        "ORDER BY o.station, o.hour_utc, o.batch_id, o.id",
        (week_start, week_end),
    ).fetchall()
    groups: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        groups.setdefault((r["station"], r["hour_utc"]), []).append({
            "batch_id": r["batch_id"],
            "batch_version_no": r["version_no"],
            "values": json.loads(r["values_json"]),
        })
    return groups


def _classify(hour_readings: list[dict], column_id: str, claim: dict | None) -> dict:
    """把同一 (station, hour) 的全部批次读数归为一个候选位置。

    claim: 该站时已被哪轮清单领用的信息（含已交读数），未领用为 None。
    """
    readings = [{
        "batch_id": x["batch_id"],
        "batch_version_no": x["batch_version_no"],
        "value": x["values"].get(column_id),  # 旧批次没有该列 → None
    } for x in hour_readings]
    # 只比较非空读数：空单元格视为“该批次未提供”，补传有值行可正常补位；
    # 两个以上不同的非空读数才算异值。
    distinct: dict[str, dict] = {}
    for rd in readings:
        if rd["value"] is not None:
            distinct.setdefault(_canon(rd["value"]), rd)
    claimed_by = claim["round_id"] if claim else None
    same_column = claim is not None and claim["column_id"] == column_id
    chosen = None
    warnings: list[str] = []
    if len(distinct) > 1:
        status, value = "conflict", None
        reason = "同站同小时存在不一致读数，列为待核并排除抽样"
        if claim:
            current = "、".join(str(rd["value"]) for rd in distinct.values())
            if same_column:
                warnings.append(
                    f"清单 #{claim['round_id']} 已交读数为 {claim['value']}，"
                    f"当前该站时存在读数 {current}，补传异值需核查")
                if _canon(claim["value"]) not in distinct:
                    warnings.append(
                        f"已交读数 {claim['value']} 已不在当前上传读数中，"
                        "原始提交被删除或替换，需核查")
                reason += f"（该站时已由清单 #{claim['round_id']} 领用，补传异值需核查）"
            else:
                reason += (f"（该站时已由清单 #{claim['round_id']} 按列 "
                           f"{claim['column_id']} 领用）")
    elif not distinct:
        status, value = "empty", None
        reason = "该小时该列没有非空读数，不可作为抽样候选"
        if claim is not None and same_column:
            if claim["value"] is None:
                reason += f"（该站时已由清单 #{claim['round_id']} 领用，当时同为无读数）"
            else:
                warnings.append(
                    f"当前无读数，但清单 #{claim['round_id']} 已交读数为 "
                    f"{claim['value']}，请核查")
                reason += "；与已交样本不一致，需核查"
        elif claim is not None:
            reason += (f"（该站时已由清单 #{claim['round_id']} 按列 "
                       f"{claim['column_id']} 领用）")
    else:
        value = next(iter(distinct.values()))["value"]
        chosen = next(rd for rd in readings if rd["value"] is not None)
        if claim is not None:
            status = "claimed"
            reason = f"前轮清单 #{claim['round_id']} 已领用该站点小时，本轮不再参与抽取"
            if not same_column:
                reason += f"（前轮抽的是列 {claim['column_id']}；站点小时一经交出不再重发）"
            elif _canon(claim["value"]) != _canon(value):
                warnings.append(
                    f"当前读数 {value} 与清单 #{claim['round_id']} 已交读数 "
                    f"{claim['value']} 不一致（已交样本被改动，需核查）")
                reason += "；已交样本读数已被改动，需核查"
        else:
            status, reason = "eligible", ""
    return {
        "status": status,
        "value": value,
        "chosen": chosen,
        "detail": {
            "readings": readings,
            "reason": reason,
            "claimed_by_round_id": claimed_by,
            "claimed_value": claim["value"] if claim else None,
            "warnings": warnings,
        },
    }


def create_round(conn, spec) -> dict:
    """生成并发布一轮抽样清单。

    spec: {"week": "2026-W47"|"2026-11-16", "column_id": "c1", "per_station": 2,
           "seed": "可选，缺省自动生成", "note": "可选"}
    """
    if not isinstance(spec, dict):
        raise DomainError("请求体必须是 JSON 对象")
    week = parse_week(spec.get("week"))
    column_id = spec.get("column_id")
    if not isinstance(column_id, str) or not column_id.strip():
        raise DomainError("请选择抽样读数的稳定列标识 column_id（如 c1）")
    column_id = column_id.strip()
    per_station = spec.get("per_station", 1)
    if isinstance(per_station, bool) or not isinstance(per_station, int):
        raise DomainError("per_station 必须是整数")
    if not 1 <= per_station <= MAX_PER_STATION:
        raise DomainError(f"per_station 必须在 1~{MAX_PER_STATION} 之间")
    note = spec.get("note") or ""
    if not isinstance(note, str):
        raise DomainError("note 必须是字符串")

    col_info = _value_for_column(conn, column_id)
    active, col = col_info["version"], col_info["column"]
    seed, seed_source = _seed_for(spec.get("seed"))

    groups = _load_week_groups(conn, week)
    claimed = {
        (r["station"], r["hour_utc"]): {
            "round_id": r["round_id"],
            "column_id": r["column_id"],
            "value": json.loads(r["value_json"]),
        }
        for r in conn.execute(
            "SELECT station, hour_utc, round_id, column_id, value_json "
            "FROM sampling_claims")
    }

    # 站号范围：本周有数据的站 ∪ 调用方显式点名的站。
    # 显式点名但本周无数据的站会出现在清单里并按全额缺额说明，而不是被静默跳过。
    week_stations = {s for (s, _h) in groups}
    extra = spec.get("stations") or []
    if not isinstance(extra, list) or any(
            not isinstance(x, str) or not x.strip() for x in extra):
        raise DomainError("stations 必须是非空字符串数组")
    requested = {x.strip() for x in extra}
    stations = sorted(week_stations | requested)
    if not stations:
        raise DomainError(
            f"{week['label']}（{week['week_start']} 起）没有任何观测数据，无法生成抽样清单",
            [f"小时范围 {week['hours'][0]} ~ {week['hours'][-1]}",
             "如需为无数据的站出清单，请用 stations 显式点名"])

    all_candidates: list[dict] = []
    summaries: list[dict] = []
    for station in stations:
        station_hours = sorted(h for (s, h) in groups if s == station)
        has_data = bool(station_hours)
        candidates = []
        for hour in station_hours:
            c = _classify(groups[(station, hour)], column_id,
                          claimed.get((station, hour)))
            c["station"], c["hour_utc"] = station, hour
            candidates.append(c)
        all_candidates.extend(candidates)

        eligible = [c for c in candidates if c["status"] == "eligible"]
        # 可复现、等概率：按 seed 决定的 sha256 排序键取前 N 个。
        eligible.sort(key=lambda c: _order_key(
            seed, week["label"], column_id, station, c["hour_utc"]))
        picked = eligible[:per_station]
        picked_ids = {c["hour_utc"] for c in picked}
        for c in candidates:
            c["selected"] = c["hour_utc"] in picked_ids

        n_conflict = sum(1 for c in candidates if c["status"] == "conflict")
        n_claimed = sum(1 for c in candidates if c["status"] == "claimed")
        n_empty = sum(1 for c in candidates if c["status"] == "empty")
        n_warn = sum(1 for c in candidates if c["detail"]["warnings"])
        shortfall = per_station - len(picked)
        reason_bits = []
        if not has_data and station in requested:
            reason_bits.append(
                f"名额不足：该站 {week['label']} 无任何观测数据，缺额 {shortfall} 个")
        elif shortfall:
            reason_bits.append(
                f"名额不足：合格且未被前轮领用的小时仅 {len(eligible)} 个，需 {per_station} 个")
        extras = []
        if n_conflict:
            extras.append(f"待核 {n_conflict} 个")
        if n_claimed:
            extras.append(f"前轮已领用 {n_claimed} 个")
        if n_empty:
            extras.append(f"空值 {n_empty} 个")
        if n_warn:
            extras.append(f"已交样本被改动/异常 {n_warn} 个")
        if extras:
            reason_bits.append("另有" + "、".join(extras))
        summaries.append({
            "station": station, "requested": per_station,
            "selected_count": len(picked), "shortfall": shortfall,
            "eligible_count": len(eligible), "conflict_count": n_conflict,
            "claimed_count": n_claimed, "empty_count": n_empty,
            "warning_count": n_warn,
            "has_data": has_data, "reason": "；".join(reason_bits), "_picked": picked,
        })

    cur = conn.execute(
        "INSERT INTO sampling_rounds (week_label, week_start, week_end, column_id, "
        " column_name, column_type, version_id, version_no, per_station, seed, "
        " seed_source, algorithm, note, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (week["label"], week["week_start"], week["week_end"], column_id,
         col["name"], col["type"], active["id"], active["version_no"], per_station,
         seed, seed_source, ALGORITHM, note.strip(), _now()),
    )
    round_id = cur.lastrowid

    for c in all_candidates:
        chosen = c["chosen"] if c["selected"] else None
        conn.execute(
            "INSERT INTO sampling_candidates (round_id, station, hour_utc, status, "
            " value_json, detail_json, selected, selected_value_json, "
            " selected_batch_id, selected_batch_version_no) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (round_id, c["station"], c["hour_utc"], c["status"],
             _dump(c["value"]) if c["value"] is not None else None,
             json.dumps(c["detail"], ensure_ascii=False, sort_keys=True),
             1 if c["selected"] else 0,
             _dump(c["value"]) if chosen else None,
             chosen["batch_id"] if chosen else None,
             chosen["batch_version_no"] if chosen else None),
        )

    for s in summaries:
        picked = s.pop("_picked")
        conn.execute(
            "INSERT INTO sampling_station_summaries (round_id, station, requested, "
            " selected_count, shortfall, eligible_count, conflict_count, claimed_count, "
            " empty_count, warning_count, has_data, reason) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (round_id, s["station"], s["requested"], s["selected_count"],
             s["shortfall"], s["eligible_count"], s["conflict_count"],
             s["claimed_count"], s["empty_count"], s["warning_count"],
             1 if s["has_data"] else 0, s["reason"]),
        )
        for c in picked:
            # 领用表主键 (station, hour_utc)：跨轮次永久占位。
            conn.execute(
                "INSERT INTO sampling_claims (station, hour_utc, round_id, column_id, "
                " value_json, batch_id, batch_version_no, created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (c["station"], c["hour_utc"], round_id, column_id,
                 _dump(c["value"]), c["chosen"]["batch_id"],
                 c["chosen"]["batch_version_no"], _now()),
            )

    return get_round(conn, round_id)


# ---------------------------------------------------------------- 导出

def round_csv(conn, round_id: int) -> tuple[str, str]:
    """返回 (文件名, CSV 文本)。导出的是清单发布时的快照读数。"""
    row = get_round_row(conn, round_id)
    if row is None:
        raise NotFoundError(f"抽样清单 #{round_id} 不存在")
    head = _round_header(row)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["清单编号", "UTC周", "周起始(UTC)", "周截止(UTC)", "站号", "UTC小时",
                "候选状态", "候选读数", "抽中", "选中读数", "选中批次",
                "选中批次结构版本", "清单结构版本", "列标识", "列名", "读数明细",
                "已交样本读数", "说明", "警示"])
    rows = conn.execute(
        "SELECT * FROM sampling_candidates WHERE round_id=? ORDER BY station, hour_utc",
        (round_id,),
    ).fetchall()
    for r in rows:
        detail = json.loads(r["detail_json"])
        readings = "；".join(
            f"#{x['batch_id']}(v{x['batch_version_no']})="
            f"{'空' if x['value'] is None else x['value']}"
            for x in detail["readings"])
        selected = bool(r["selected"])
        value = json.loads(r["selected_value_json"]) if r["selected_value_json"] else None
        w.writerow([
            head["id"], head["week_label"], head["week_start"], head["week_end"],
            r["station"], r["hour_utc"], r["status"],
            "" if r["value_json"] is None else json.loads(r["value_json"]),
            "是" if selected else "否",
            "" if value is None else value,
            r["selected_batch_id"] if selected else "",
            f"v{r['selected_batch_version_no']}" if selected else "",
            f"v{head['structure_version_no']}",
            head["column"]["column_id"], head["column"]["name"],
            readings, "" if detail.get("claimed_value") is None else detail["claimed_value"],
            detail.get("reason", ""), "；".join(detail.get("warnings", [])),
        ])
    return (f"sampling-{head['week_label']}-round-{head['id']}.csv",
            "﻿" + out.getvalue())  # BOM 便于 Excel 直接打开中文表头
