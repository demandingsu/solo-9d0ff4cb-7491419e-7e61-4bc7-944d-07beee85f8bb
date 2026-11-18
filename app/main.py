"""FastAPI 入口：网页 + JSON API。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import Body, FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

from . import core, sampling
from .db import init_db, session

MAX_CSV_BYTES = 20 * 1024 * 1024


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="环境监测小时观测数据平台", lifespan=lifespan)
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
)


@app.exception_handler(core.DomainError)
async def domain_error_handler(request: Request, exc: core.DomainError):
    status = 404 if isinstance(exc, core.NotFoundError) else 400
    return JSONResponse(status_code=status,
                        content={"detail": exc.message, "errors": exc.errors})


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/api/versions")
def api_list_versions():
    with session() as conn:
        return {"versions": core.list_versions(conn)}


@app.get("/api/versions/current")
def api_current_version():
    with session() as conn:
        v = core.get_active_version(conn)
        if v is None:
            raise core.NotFoundError("尚未发布任何结构版本")
        return core.version_with_columns(conn, v)


@app.get("/api/versions/{version_no}")
def api_get_version(version_no: int):
    with session() as conn:
        v = core.get_version(conn, version_no)
        if v is None:
            raise core.NotFoundError(f"版本 v{version_no} 不存在")
        return core.version_with_columns(conn, v)


@app.post("/api/versions", status_code=201)
def api_publish_version(spec: dict = Body(...)):
    with session() as conn:
        return core.publish_version(conn, spec)


@app.post("/api/batches", status_code=201)
async def api_upload_batch(file: UploadFile = File(...)):
    raw = await file.read()
    if len(raw) > MAX_CSV_BYTES:
        raise core.DomainError("CSV 文件过大（上限 20MB）")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise core.DomainError("CSV 必须是 UTF-8 编码")
    with session() as conn:
        return core.upload_csv(conn, file.filename or "", text)


@app.get("/api/batches")
def api_list_batches():
    with session() as conn:
        return {"batches": core.list_batches(conn)}


@app.get("/api/observations")
def api_observations(
    station: str | None = None,
    hour_from: str | None = None,
    hour_to: str | None = None,
    version_no: int | None = None,
    limit: int = 1000,
):
    limit = max(1, min(limit, 10000))
    try:
        hf = core.normalize_hour(hour_from) if hour_from else None
        ht = core.normalize_hour(hour_to) if hour_to else None
    except ValueError as e:
        raise core.DomainError(str(e))
    with session() as conn:
        return core.query_observations(
            conn, station=station, hour_from=hf, hour_to=ht,
            version_no=version_no, limit=limit,
        )


# ---------------------------------------------------------------- 抽样清单

@app.get("/api/sampling/meta")
def api_sampling_meta():
    """页面辅助：当前 UTC 周、抽样列候选、已领用样本。"""
    week = sampling.current_week()
    with session() as conn:
        active = core.get_active_version(conn)
        columns = []
        if active is not None:
            columns = [c for c in core.version_columns(conn, active["id"])
                       if c["type"] not in ("station", "hour")]
        return {
            "current_week": week["label"],
            "week_start": week["week_start"],
            "week_end": week["week_end"],
            "active_version_no": active["version_no"] if active else None,
            "columns": columns,
            "claims": sampling.list_claims(conn),
        }


@app.get("/api/sampling/rounds")
def api_list_rounds(week: str | None = None):
    label = sampling.parse_week(week)["label"] if week else None
    with session() as conn:
        return {"rounds": sampling.list_rounds(conn, label)}


@app.post("/api/sampling/rounds", status_code=201)
def api_create_round(spec: dict = Body(...)):
    with session() as conn:
        return sampling.create_round(conn, spec)


@app.get("/api/sampling/rounds/{round_id}")
def api_get_round(round_id: int):
    with session() as conn:
        return sampling.get_round(conn, round_id)


@app.get("/api/sampling/rounds/{round_id}/csv")
def api_round_csv(round_id: int):
    with session() as conn:
        filename, text = sampling.round_csv(conn, round_id)
    return PlainTextResponse(
        text, media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
