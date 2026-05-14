from __future__ import annotations

import os
from dataclasses import dataclass


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


@dataclass
class Config:
    bot_token: str
    target_channel_id: int | str
    allowed_user_ids: set[int]
    tmdb_api_key: str
    llm_api_key: str
    llm_base_url: str
    llm_model: str
    llm_auto_post: bool
    llm_debug: bool
    telegram_min_interval: float
    telegram_file_interval: float
    telegram_max_retries: int
    queue_notify_every: int
    deep_scan_enabled: bool
    deep_scan_timeout: float
    deep_scan_max_mb: int
    database_path: str


def load_config() -> Config:
    token = os.getenv("BOT_TOKEN", "").strip()
    channel = os.getenv("TARGET_CHANNEL_ID", "").strip()
    tmdb_key = os.getenv("TMDB_API_KEY", "").strip()
    llm_key = os.getenv("LLM_API_KEY", "").strip()
    llm_url = os.getenv("LLM_BASE_URL", "").strip()
    llm_model = os.getenv("LLM_MODEL", "").strip()
    llm_auto_post = os.getenv("LLM_AUTO_POST", "false").strip().lower() == "true"
    llm_debug = os.getenv("LLM_DEBUG", "false").strip().lower() == "true"
    telegram_min_interval = env_float("TELEGRAM_MIN_INTERVAL", 1.2)
    telegram_file_interval = env_float("TELEGRAM_FILE_INTERVAL", 2.0)
    telegram_max_retries = env_int("TELEGRAM_MAX_RETRIES", 8)
    queue_notify_every = env_int("QUEUE_NOTIFY_EVERY", 25)
    deep_scan_enabled = os.getenv("DEEP_SCAN_ENABLED", "true").strip().lower() == "true"
    deep_scan_timeout = env_float("DEEP_SCAN_TIMEOUT", 20.0)
    deep_scan_max_mb = env_int("DEEP_SCAN_MAX_MB", 2048)
    database_path = os.getenv("DATABASE_PATH", "/app/data/bot.sqlite3").strip()

    if not token:
        raise RuntimeError("Falta BOT_TOKEN en las variables de entorno.")
    if not channel:
        raise RuntimeError("Falta TARGET_CHANNEL_ID en las variables de entorno.")

    allowed_raw = os.getenv("ALLOWED_USER_IDS", "").strip()
    allowed = {int(item.strip()) for item in allowed_raw.split(",") if item.strip()} if allowed_raw else set()

    try:
        target_channel_id: int | str = int(channel)
    except ValueError:
        target_channel_id = channel

    return Config(
        bot_token=token, 
        target_channel_id=target_channel_id, 
        allowed_user_ids=allowed, 
        tmdb_api_key=tmdb_key,
        llm_api_key=llm_key,
        llm_base_url=llm_url,
        llm_model=llm_model,
        llm_auto_post=llm_auto_post,
        llm_debug=llm_debug,
        telegram_min_interval=telegram_min_interval,
        telegram_file_interval=telegram_file_interval,
        telegram_max_retries=telegram_max_retries,
        queue_notify_every=queue_notify_every,
        deep_scan_enabled=deep_scan_enabled,
        deep_scan_timeout=deep_scan_timeout,
        deep_scan_max_mb=deep_scan_max_mb,
        database_path=database_path,
    )


def parse_bool_arg(value: str) -> bool | None:
    normalized = value.strip().lower()
    if normalized in {"on", "true", "1", "yes", "si", "sí", "activar", "enable"}:
        return True
    if normalized in {"off", "false", "0", "no", "desactivar", "disable"}:
        return False
    return None


def parse_target_channel(value: str) -> int | str | None:
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        if value.startswith("@"):
            return value
    return None
