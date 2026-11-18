"""SQLite 连接与事务管理。

所有读写都通过 session() 进行：全局可重入锁 + 单连接事务。
发布 / 上传 / 查询任一操作要么整体完成、要么整体回滚，
保证"结构"与"数据"永远成对出现，交错调用不会混用。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version_no  INTEGER NOT NULL UNIQUE,
    status      TEXT NOT NULL CHECK (status IN ('active', 'superseded')),
    note        TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schema_columns (
    version_id  INTEGER NOT NULL REFERENCES schema_versions(id) ON DELETE CASCADE,
    column_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL,
    position    INTEGER NOT NULL,
    nullable    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (version_id, column_id),
    UNIQUE (version_id, name)
);
CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id  INTEGER NOT NULL REFERENCES schema_versions(id),
    filename    TEXT NOT NULL DEFAULT '',
    row_count   INTEGER NOT NULL,
    uploaded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    station     TEXT NOT NULL,
    hour_utc    TEXT NOT NULL,
    values_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_obs_station_hour ON observations (station, hour_utc);
CREATE INDEX IF NOT EXISTS idx_obs_batch ON observations (batch_id);
CREATE INDEX IF NOT EXISTS idx_obs_hour ON observations (hour_utc);

-- 抽样清单：每个轮次一旦发布即为快照，候选范围与选中读数不再随补传/换版变化。
CREATE TABLE IF NOT EXISTS sampling_rounds (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    week_label    TEXT NOT NULL,
    week_start    TEXT NOT NULL,
    week_end      TEXT NOT NULL,
    column_id     TEXT NOT NULL,
    column_name   TEXT NOT NULL,
    column_type   TEXT NOT NULL,
    version_id    INTEGER NOT NULL REFERENCES schema_versions(id),
    version_no    INTEGER NOT NULL,
    per_station   INTEGER NOT NULL,
    seed          TEXT NOT NULL,
    seed_source   TEXT NOT NULL CHECK (seed_source IN ('provided', 'generated')),
    algorithm     TEXT NOT NULL,
    note          TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sampling_candidates (
    round_id       INTEGER NOT NULL REFERENCES sampling_rounds(id) ON DELETE CASCADE,
    station        TEXT NOT NULL,
    hour_utc       TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('eligible', 'conflict', 'claimed', 'empty')),
    value_json     TEXT,
    detail_json    TEXT NOT NULL,
    selected       INTEGER NOT NULL DEFAULT 0,
    selected_value_json      TEXT,
    selected_batch_id        INTEGER,
    selected_batch_version_no INTEGER,
    PRIMARY KEY (round_id, station, hour_utc)
);
CREATE TABLE IF NOT EXISTS sampling_claims (
    station           TEXT NOT NULL,
    hour_utc          TEXT NOT NULL,
    round_id          INTEGER NOT NULL REFERENCES sampling_rounds(id),
    column_id         TEXT NOT NULL,
    value_json        TEXT NOT NULL,
    batch_id          INTEGER NOT NULL,
    batch_version_no  INTEGER NOT NULL,
    created_at        TEXT NOT NULL,
    PRIMARY KEY (station, hour_utc)
);
CREATE TABLE IF NOT EXISTS sampling_station_summaries (
    round_id        INTEGER NOT NULL REFERENCES sampling_rounds(id) ON DELETE CASCADE,
    station         TEXT NOT NULL,
    requested       INTEGER NOT NULL,
    selected_count  INTEGER NOT NULL,
    shortfall       INTEGER NOT NULL,
    eligible_count  INTEGER NOT NULL,
    conflict_count  INTEGER NOT NULL,
    claimed_count   INTEGER NOT NULL,
    empty_count     INTEGER NOT NULL,
    warning_count   INTEGER NOT NULL DEFAULT 0,
    has_data        INTEGER NOT NULL,
    reason          TEXT NOT NULL,
    PRIMARY KEY (round_id, station)
);
CREATE INDEX IF NOT EXISTS idx_sampling_claims_round ON sampling_claims (round_id);
CREATE INDEX IF NOT EXISTS idx_sampling_rounds_week ON sampling_rounds (week_label, id);
"""


def db_path() -> str:
    return os.environ.get("DB_PATH") or os.path.join(os.getcwd(), "data", "obs.db")


def connect() -> sqlite3.Connection:
    path = db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.Error:
        pass
    return conn


def init_db() -> None:
    with _lock:
        with connect() as conn:
            conn.executescript(SCHEMA)


@contextmanager
def session():
    """全局锁 + 单连接事务：正常退出提交，异常回滚并关闭连接。

    显式 BEGIN IMMEDIATE：同一操作内的读取（候选范围、已领样本）与写入（清单、
    领用记录）处在同一个 SQLite 事务快照里，且与其它进程的上传 / 换版串行，
    不会出现“读到旧状态、写入新状态”的混合结果。
    """
    with _lock:
        conn = connect()
        try:
            conn.isolation_level = None  # 自行控制事务，避免隐式 BEGIN
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass  # 连接已不可用，事务随关闭一并丢弃
                raise
            try:
                conn.execute("COMMIT")
            except sqlite3.Error:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        finally:
            conn.close()
