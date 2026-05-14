from __future__ import annotations

import os
import re
import sqlite3
import time

from app.config import Config, parse_bool_arg, parse_target_channel


def init_database(config: Config) -> None:
    parent = os.path.dirname(config.database_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with sqlite3.connect(config.database_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS published_media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT NOT NULL UNIQUE,
                caption TEXT NOT NULL,
                media_type TEXT,
                name TEXT,
                year TEXT,
                season TEXT,
                episode TEXT,
                quality TEXT,
                optional TEXT,
                file_name TEXT,
                file_id TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_review (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                file_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                caption TEXT,
                width INTEGER,
                height INTEGER,
                quality TEXT,
                reason TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )


def save_setting(config: Config, key: str, value: object) -> None:
    with sqlite3.connect(config.database_path) as conn:
        conn.execute(
            """
            INSERT INTO settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, str(value), int(time.time())),
        )


def load_settings(config: Config) -> dict[str, str]:
    with sqlite3.connect(config.database_path) as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    return {key: value for key, value in rows}


def apply_persisted_settings(config: Config) -> None:
    settings = load_settings(config)
    if "target_channel_id" in settings:
        parsed = parse_target_channel(settings["target_channel_id"])
        if parsed is not None:
            config.target_channel_id = parsed
    if "llm_auto_post" in settings:
        parsed = parse_bool_arg(settings["llm_auto_post"])
        if parsed is not None:
            config.llm_auto_post = parsed
    if "llm_debug" in settings:
        parsed = parse_bool_arg(settings["llm_debug"])
        if parsed is not None:
            config.llm_debug = parsed
    if "telegram_min_interval" in settings:
        config.telegram_min_interval = float(settings["telegram_min_interval"])
    if "telegram_file_interval" in settings:
        config.telegram_file_interval = float(settings["telegram_file_interval"])
    if "queue_notify_every" in settings:
        config.queue_notify_every = int(settings["queue_notify_every"])


def dedupe_key(caption: str) -> str:
    return re.sub(r"\s+", " ", caption.strip().lower())


def already_published(config: Config, caption: str) -> bool:
    with sqlite3.connect(config.database_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM published_media WHERE dedupe_key = ? LIMIT 1",
            (dedupe_key(caption),),
        ).fetchone()
    return row is not None


def register_published(config: Config, data: dict, caption: str) -> None:
    with sqlite3.connect(config.database_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO published_media (
                dedupe_key, caption, media_type, name, year, season, episode,
                quality, optional, file_name, file_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dedupe_key(caption),
                caption,
                data.get("media_type"),
                data.get("name"),
                data.get("year"),
                data.get("season"),
                data.get("episode"),
                data.get("quality"),
                data.get("optional"),
                data.get("file_name"),
                data.get("file_id"),
                int(time.time()),
            ),
        )


def save_pending_review(config: Config, user_id: int, video_info: dict, caption: str, quality: str | None, reason: str) -> int:
    now = int(time.time())
    with sqlite3.connect(config.database_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO pending_review (
                user_id, kind, file_id, file_name, caption, width, height,
                quality, reason, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                user_id,
                video_info.get("kind"),
                video_info.get("file_id"),
                video_info.get("file_name"),
                caption,
                video_info.get("width"),
                video_info.get("height"),
                quality,
                reason,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)


def pending_review_count(config: Config, user_id: int) -> int:
    with sqlite3.connect(config.database_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM pending_review WHERE user_id = ? AND status = 'pending'",
            (user_id,),
        ).fetchone()
    return int(row[0]) if row else 0


def get_next_pending_review(config: Config, user_id: int) -> dict | None:
    with sqlite3.connect(config.database_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT * FROM pending_review
            WHERE user_id = ? AND status = 'pending'
            ORDER BY id ASC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def mark_pending_review_done(config: Config, pending_id: int) -> None:
    with sqlite3.connect(config.database_path) as conn:
        conn.execute(
            "UPDATE pending_review SET status = 'done', updated_at = ? WHERE id = ?",
            (int(time.time()), pending_id),
        )
