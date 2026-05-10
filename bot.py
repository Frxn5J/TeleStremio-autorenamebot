from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
import urllib.parse
from dataclasses import dataclass
from typing import Literal

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from openai import AsyncOpenAI

# Diccionario global para manejar las colas por usuario
user_queues: dict[int, list[Message]] = {}
user_processing: dict[int, bool] = {}
user_processed_counts: dict[int, int] = {}
telegram_send_lock = asyncio.Lock()
last_telegram_send_at = 0.0


MediaType = Literal["movie", "series"]


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


def init_database(config: "Config") -> None:
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


def save_setting(config: "Config", key: str, value: object) -> None:
    with sqlite3.connect(config.database_path) as conn:
        conn.execute(
            """
            INSERT INTO settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, str(value), int(time.time())),
        )


def load_settings(config: "Config") -> dict[str, str]:
    with sqlite3.connect(config.database_path) as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    return {key: value for key, value in rows}


def apply_persisted_settings(config: "Config") -> None:
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


def already_published(config: "Config", caption: str) -> bool:
    with sqlite3.connect(config.database_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM published_media WHERE dedupe_key = ? LIMIT 1",
            (dedupe_key(caption),),
        ).fetchone()
    return row is not None


def register_published(config: "Config", data: dict, caption: str) -> None:
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


def save_pending_review(config: "Config", user_id: int, video_info: dict, caption: str, quality: str | None, reason: str) -> int:
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


def pending_review_count(config: "Config", user_id: int) -> int:
    with sqlite3.connect(config.database_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM pending_review WHERE user_id = ? AND status = 'pending'",
            (user_id,),
        ).fetchone()
    return int(row[0]) if row else 0


def get_next_pending_review(config: "Config", user_id: int) -> dict | None:
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


def mark_pending_review_done(config: "Config", pending_id: int) -> None:
    with sqlite3.connect(config.database_path) as conn:
        conn.execute(
            "UPDATE pending_review SET status = 'done', updated_at = ? WHERE id = ?",
            (int(time.time()), pending_id),
        )


async def publish_media(bot: Bot, config: "Config", data: dict, caption: str) -> bool:
    if already_published(config, caption):
        logging.info("Duplicate skipped: %s", caption)
        return False
    await safe_send_media(bot, config.target_channel_id, data, caption, config=config)
    register_published(config, data, caption)
    return True


async def telegram_rate_limit(config: "Config") -> None:
    global last_telegram_send_at
    async with telegram_send_lock:
        now = asyncio.get_running_loop().time()
        wait_for = config.telegram_min_interval - (now - last_telegram_send_at)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        last_telegram_send_at = asyncio.get_running_loop().time()


async def safe_answer(target: Message, text: str, **kwargs) -> None:
    config = kwargs.pop("config", None)
    max_retries = config.telegram_max_retries if config else env_int("TELEGRAM_MAX_RETRIES", 8)
    for attempt in range(max_retries):
        try:
            if config:
                await telegram_rate_limit(config)
            await target.answer(text, **kwargs)
            return
        except TelegramRetryAfter as exc:
            logging.warning("Telegram flood control sending message, retry after %s seconds", exc.retry_after)
            await asyncio.sleep(exc.retry_after + 1)
        except TelegramNetworkError as exc:
            logging.warning("Telegram timeout sending message, attempt %s/%s: %s", attempt + 1, max_retries, exc)
            await asyncio.sleep(min(60, 2 ** attempt))
    logging.error("Failed to send Telegram message after %s attempts", max_retries)


async def safe_send_message(bot: Bot, chat_id: int | str, text: str, **kwargs) -> None:
    config = kwargs.pop("config", None)
    max_retries = config.telegram_max_retries if config else env_int("TELEGRAM_MAX_RETRIES", 8)
    for attempt in range(max_retries):
        try:
            if config:
                await telegram_rate_limit(config)
            await bot.send_message(chat_id, text, **kwargs)
            return
        except TelegramRetryAfter as exc:
            logging.warning("Telegram flood control sending direct message, retry after %s seconds", exc.retry_after)
            await asyncio.sleep(exc.retry_after + 1)
        except TelegramNetworkError as exc:
            logging.warning("Telegram timeout sending direct message, attempt %s/%s: %s", attempt + 1, max_retries, exc)
            await asyncio.sleep(min(60, 2 ** attempt))
    logging.error("Failed to send Telegram direct message after %s attempts", max_retries)


async def safe_send_media(bot: Bot, chat_id: int | str, data: dict, caption: str, config: "Config" | None = None) -> None:
    max_retries = config.telegram_max_retries if config else env_int("TELEGRAM_MAX_RETRIES", 8)
    for attempt in range(max_retries):
        try:
            if config:
                await telegram_rate_limit(config)
            if data["kind"] == "video":
                await bot.send_video(chat_id, data["file_id"], caption=caption)
            else:
                await bot.send_document(chat_id, data["file_id"], caption=caption)
            return
        except TelegramRetryAfter as exc:
            logging.warning("Telegram flood control sending media, retry after %s seconds", exc.retry_after)
            await asyncio.sleep(exc.retry_after + 1)
        except TelegramNetworkError as exc:
            logging.warning("Telegram timeout sending media, attempt %s/%s: %s", attempt + 1, max_retries, exc)
            await asyncio.sleep(min(60, 2 ** attempt))

    raise TimeoutError("Telegram media send timeout after retries")


async def safe_send_temp_media(bot: Bot, chat_id: int | str, data: dict, config: "Config") -> Message | None:
    max_retries = config.telegram_max_retries
    for attempt in range(max_retries):
        try:
            await telegram_rate_limit(config)
            if data["kind"] == "video":
                return await bot.send_video(chat_id, data["file_id"], caption="Archivo para revisar")
            return await bot.send_document(chat_id, data["file_id"], caption="Archivo para revisar")
        except TelegramRetryAfter as exc:
            logging.warning("Telegram flood control sending temp media, retry after %s seconds", exc.retry_after)
            await asyncio.sleep(exc.retry_after + 1)
        except TelegramNetworkError as exc:
            logging.warning("Telegram timeout sending temp media, attempt %s/%s: %s", attempt + 1, max_retries, exc)
            await asyncio.sleep(min(60, 2 ** attempt))
        except (TelegramBadRequest, TelegramForbiddenError) as exc:
            logging.warning("Could not send temp media: %s", exc)
            return None
    logging.error("Failed to send temporary media after %s attempts", max_retries)
    return None


async def show_temp_review_media(message: Message, state: FSMContext, bot: Bot, config: "Config", data: dict) -> None:
    existing = (await state.get_data()).get("temp_review_message_id")
    if existing:
        return
    sent = await safe_send_temp_media(bot, message.chat.id, data, config)
    if sent:
        await state.update_data(temp_review_message_id=sent.message_id)


async def cleanup_temp_review_media(bot: Bot, state: FSMContext, chat_id: int | str) -> None:
    data = await state.get_data()
    message_id = data.get("temp_review_message_id")
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id, int(message_id))
    except (TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError) as exc:
        logging.warning("Could not delete temporary review media: %s", exc)


async def after_file_processed(user_id: int, state: FSMContext, bot: Bot, config: "Config") -> None:
    await cleanup_temp_review_media(bot, state, user_id)
    user_processed_counts[user_id] = user_processed_counts.get(user_id, 0) + 1
    await asyncio.sleep(config.telegram_file_interval)
    await process_next_in_queue(user_id, state, bot, config)

VIDEO_EXTENSIONS = {".mkv", ".mp4"}
MULTIPART_RE = re.compile(r"(?:^|[\s._-])(?:part\s*\d+|cd\s*\d+)(?:[\s._-]|$)", re.IGNORECASE)
SEASON_RE = re.compile(r"^S\d{2,}$", re.IGNORECASE)
EPISODE_RE = re.compile(r"^E\d{2,}$", re.IGNORECASE)
QUALITY_RE = re.compile(r"(2160p|1440p|1080p|720p|576p|540p|480p|360p)", re.IGNORECASE)
GENERIC_TITLE_RE = re.compile(
    r"^(?:video|videos|vid|movie|movies|film|pelicula|película|archivo|file|document|documento|clip|media|telegram|download|upload|untitled|sin titulo|sin título)\s*\d*$",
    re.IGNORECASE,
)
SEASON_EPISODE_TEXT_RE = re.compile(
    r"(?P<title>.*?)"
    r"(?:\b(?:temp|temporada|season|t)\.?\s*(?P<season>\d{1,2})\b)"
    r"[\s._-]*"
    r"(?:\b(?:ep|episodio|episode|e)\.?\s*(?P<episode>\d{1,3})\b)"
    r"(?P<episode_title>.*)",
    re.IGNORECASE,
)
SXX_EXX_RE = re.compile(
    r"(?P<title>.*?)(?:S(?P<season>\d{1,2})\s*E(?P<episode>\d{1,3}))(?P<episode_title>.*)",
    re.IGNORECASE,
)


class MediaForm(StatesGroup):
    confirming_llm = State()
    confirming_tmdb = State()
    choosing_type = State()
    movie_name = State()
    movie_year = State()
    movie_quality = State()
    movie_optional = State()
    series_name = State()
    series_season = State()
    series_episode = State()
    series_quality = State()
    series_optional = State()
    confirming = State()


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
        database_path=database_path,
    )


def llm_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Enviar al canal", callback_data="llm:send"),
                InlineKeyboardButton(text="Editar manual", callback_data="llm:edit"),
            ]
        ]
    )


def tmdb_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Sí, es correcto", callback_data="tmdb:yes"),
                InlineKeyboardButton(text="No, continuar manual", callback_data="tmdb:no"),
            ]
        ]
    )


def type_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Película", callback_data="type:movie")
    builder.button(text="Serie", callback_data="type:series")
    builder.adjust(2)
    return builder.as_markup()


def confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Enviar al canal", callback_data="confirm:send"),
                InlineKeyboardButton(text="Cancelar", callback_data="confirm:cancel"),
            ]
        ]
    )


def get_video_info(message: Message) -> dict | None:
    if message.video:
        return {
            "kind": "video",
            "file_id": message.video.file_id,
            "file_name": message.video.file_name or "video.mp4",
            "width": message.video.width,
            "height": message.video.height,
        }

    if message.document and message.document.mime_type and message.document.mime_type.startswith("video/"):
        return {
            "kind": "document",
            "file_id": message.document.file_id,
            "file_name": message.document.file_name or "video.mkv",
            "width": None,
            "height": None,
        }

    if message.document and get_extension(message.document.file_name or "") in VIDEO_EXTENSIONS:
        return {
            "kind": "document",
            "file_id": message.document.file_id,
            "file_name": message.document.file_name or "video.mkv",
            "width": None,
            "height": None,
        }

    return None


def get_extension(file_name: str) -> str:
    _, ext = os.path.splitext(file_name.lower())
    return ext


def clean_filename_for_search(file_name: str) -> str:
    name, _ = os.path.splitext(file_name)
    # Remove quality, codecs, years, episodes etc to get just the title
    name = re.sub(r"(19|20)\d{2}.*", "", name) # Remove year and everything after
    name = re.sub(r"S\d{2}E\d{2}.*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"(2160p|1440p|1080p|720p|576p|540p|480p|360p).*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[\._-]", " ", name)
    return name.strip()


def is_generic_title(value: str) -> bool:
    normalized = normalize_metadata_text(value).lower()
    normalized = QUALITY_RE.sub("", normalized)
    normalized = re.sub(r"\b(19|20)\d{2}\b", "", normalized)
    normalized = clean_text(normalized).strip(" .-_")
    if not normalized:
        return True
    if GENERIC_TITLE_RE.fullmatch(normalized):
        return True
    return len(normalized) < 3


def useful_search_text(value: str) -> str | None:
    cleaned = clean_filename_for_search(value)
    if is_generic_title(cleaned):
        return None
    words = re.findall(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]+", cleaned)
    if len(words) < 2 and not re.search(r"\b(19|20)\d{2}\b", value):
        return None
    return cleaned


def choose_search_query(file_name: str, caption: str) -> str | None:
    return useful_search_text(caption) or useful_search_text(file_name)


def normalize_metadata_text(value: str) -> str:
    value = os.path.splitext(value)[0]
    value = re.sub(r"[._-]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def format_season(value: str | int | None) -> str | None:
    if value is None:
        return None
    if match := re.search(r"\d+", str(value)):
        return f"S{int(match.group(0)):02d}"
    return None


def video_info_from_pending(row: dict) -> dict:
    return {
        "kind": row["kind"],
        "file_id": row["file_id"],
        "file_name": row["file_name"],
        "width": row.get("width"),
        "height": row.get("height"),
    }


def format_episode(value: str | int | None) -> str | None:
    if value is None:
        return None
    if match := re.search(r"\d+", str(value)):
        return f"E{int(match.group(0)):02d}"
    return None


def clean_detected_name(value: str) -> str:
    value = normalize_metadata_text(value)
    value = QUALITY_RE.sub("", value)
    value = re.sub(r"\b(19|20)\d{2}\b", "", value)
    value = re.sub(r"\b(?:mp4|mkv|avi|web-dl|webrip|bluray|x264|x265|h264|h265)\b", "", value, flags=re.IGNORECASE)
    return clean_text(value).strip(" .")


def local_parse_series(text: str) -> dict | None:
    normalized = normalize_metadata_text(text)
    match = SEASON_EPISODE_TEXT_RE.search(normalized) or SXX_EXX_RE.search(normalized)
    if not match:
        lower = normalized.lower()
        temp_match = re.search(r"\b(?:temp|temporada|season|t)\.?\s*(\d{1,2})\b", lower, re.IGNORECASE)
        ep_match = re.search(r"\b(?:ep|episodio|episode|e)\.?\s*(\d{1,3})\b", lower, re.IGNORECASE)
        if not temp_match or not ep_match:
            return None
        title = normalized[: temp_match.start()]
        episode_title = normalized[ep_match.end() :]
        name = clean_detected_name(title)
        season = format_season(temp_match.group(1))
        episode = format_episode(ep_match.group(1))
        if not name or not season or not episode:
            return None
        return {
            "media_type": "series",
            "name": name,
            "season": season,
            "episode": episode,
            "optional": clean_detected_name(episode_title),
        }

    if not match:
        return None

    name = clean_detected_name(match.group("title"))
    season = format_season(match.group("season"))
    episode = format_episode(match.group("episode"))
    if not name or not season or not episode:
        return None

    episode_title = clean_detected_name(match.groupdict().get("episode_title") or "")
    return {
        "media_type": "series",
        "name": name,
        "season": season,
        "episode": episode,
        "optional": episode_title,
    }


def local_parse_movie(text: str) -> dict | None:
    normalized = normalize_metadata_text(text)
    match = re.search(r"(?P<title>.*?)(?:\s+[\(\[-]?)(?P<year>(?:19|20)\d{2})(?:[\)\]-]?)(?P<rest>.*)$", normalized)
    if not match:
        return None

    title = clean_detected_name(match.group("title"))
    year = match.group("year")
    optional = clean_detected_name(match.group("rest") or "")
    if not title or not year:
        return None
    return {
        "media_type": "movie",
        "name": title,
        "year": year,
        "optional": optional,
    }


def local_parse_media(text: str) -> dict | None:
    return local_parse_series(text) or local_parse_movie(text)


def local_parse_from_sources(file_name: str, caption: str) -> dict | None:
    sources = []
    if caption and not is_generic_title(caption):
        sources.append(caption)
    if file_name and not is_generic_title(file_name):
        sources.append(file_name)
    if len(sources) == 2:
        sources.append(f"{file_name} {caption}")
    for source in sources:
        parsed = local_parse_media(source)
        if parsed:
            return parsed
    return None


def normalize_llm_data(data: dict, fallback_quality: str | None, file_name: str, caption: str) -> dict:
    result = dict(data)
    local_data = local_parse_from_sources(file_name, caption)
    if local_data and result.get("media_type") == local_data.get("media_type"):
        for key in ("name", "year", "season", "episode"):
            if not result.get(key) and local_data.get(key):
                result[key] = local_data.get(key)
        if not result.get("optional"):
            result["optional"] = local_data.get("optional", "")

    if result.get("media_type") == "series":
        result["season"] = format_season(result.get("season")) or result.get("season")
        result["episode"] = format_episode(result.get("episode")) or result.get("episode")

    if not result.get("quality") and fallback_quality:
        result["quality"] = fallback_quality
    if result.get("quality") is None:
        result["quality"] = ""
    if result.get("optional") is None:
        result["optional"] = ""

    return result


def required_fields_missing(data: dict) -> list[str]:
    if data.get("media_type") == "movie":
        return [key for key in ("name", "year", "quality") if not data.get(key)]
    if data.get("media_type") == "series":
        return [key for key in ("name", "season", "episode", "quality") if not data.get(key)]
    return ["media_type"]


async def ask_missing_required_fields(message: Message, state: FSMContext, bot: Bot, config: Config, data: dict, missing: list[str]) -> bool:
    await show_temp_review_media(message, state, bot, config, data)
    if data.get("media_type") == "movie":
        if "year" in missing:
            await state.set_state(MediaForm.movie_year)
            await safe_answer(message, current_file_label(data) + f"Película detectada: <b>{data.get('name', '')}</b>\n\nAño de estreno. Ejemplo: 2023")
            return True
        if "quality" in missing:
            await state.set_state(MediaForm.movie_quality)
            await ask_movie_quality(message, state)
            return True

    if data.get("media_type") == "series":
        if "season" in missing:
            await state.set_state(MediaForm.series_season)
            await safe_answer(message, current_file_label(data) + "Temporada con S y mínimo 2 dígitos. Ejemplo: S01")
            return True
        if "episode" in missing:
            await state.set_state(MediaForm.series_episode)
            await safe_answer(message, current_file_label(data) + "Episodio con E y mínimo 2 dígitos. Ejemplo: E04")
            return True
        if "quality" in missing:
            await state.set_state(MediaForm.series_quality)
            await ask_series_quality(message, state)
            return True

    return False


async def handle_detected_media(
    user_id: int,
    message: Message,
    state: FSMContext,
    bot: Bot,
    config: Config,
    video_info: dict,
    detected_data: dict,
) -> bool:
    await state.update_data(**detected_data)
    merged_data = {**(await state.get_data()), **detected_data}
    merged_data = await enrich_missing_from_tmdb(merged_data, config)
    await state.update_data(**merged_data)
    missing = required_fields_missing(merged_data)
    if missing:
        logging.info("Detected partial metadata, missing fields: %s", ", ".join(missing))
        if config.llm_auto_post:
            pending_id = save_pending_review(config, user_id, video_info, message.caption or "", merged_data.get("quality"), f"missing: {', '.join(missing)}")
            logging.info("Saved pending review #%s for %s", pending_id, video_info.get("file_name"))
            await state.clear()
            await after_file_processed(user_id, state, bot, config)
            return True
        if await ask_missing_required_fields(message, state, bot, config, merged_data, missing):
            return True
        return False

    try:
        caption = build_caption(merged_data)
    except KeyError as exc:
        logging.error("Detected metadata missing key for build_caption: %s", exc)
        return False

    await state.update_data(caption=caption)
    if config.llm_auto_post:
        try:
            published = await publish_media(bot, config, merged_data, caption)
            if not published:
                await safe_answer(message, f"⏭️ Duplicado omitido: <code>{caption}</code>", config=config)
            await state.clear()
            await after_file_processed(user_id, state, bot, config)
            return True
        except (TelegramBadRequest, TelegramForbiddenError, TimeoutError) as exc:
            await safe_answer(message, f"❌ Error enviando auto-post al canal: {exc}\nPasando a modo manual.", config=config)

    await state.set_state(MediaForm.confirming_llm)
    await safe_answer(message, f"Detección automática:\n<code>{caption}</code>\n\n¿Deseas enviarlo directamente o editarlo manualmente?", reply_markup=llm_confirm_keyboard())
    return True


def extract_llm_content(response: object) -> str:
    if isinstance(response, str):
        if "data:" in response and "chat.completion.chunk" in response:
            return extract_sse_content(response)
        return response

    if isinstance(response, dict):
        choices = response.get("choices") or []
        if choices:
            message = choices[0].get("message") or {}
            content = message.get("content")
            if content is not None:
                return str(content)
        content = response.get("content") or response.get("text")
        return str(content) if content is not None else json.dumps(response, ensure_ascii=False)

    choices = getattr(response, "choices", None)
    if choices:
        message = getattr(choices[0], "message", None)
        content = getattr(message, "content", None) if message else None
        if content is not None:
            return str(content)

    return str(response)


def extract_sse_content(text: str) -> str:
    parts: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue

        payload = line.removeprefix("data:").strip()
        if not payload or payload == "[DONE]":
            continue

        try:
            chunk = json.loads(payload)
        except json.JSONDecodeError:
            continue

        for choice in chunk.get("choices", []):
            delta = choice.get("delta") or {}
            message = choice.get("message") or {}
            content = delta.get("content") or message.get("content")
            if content:
                parts.append(content)

    return "".join(parts) if parts else text


def extract_json_object(text: str) -> dict:
    content = text.strip()
    if content.startswith("```"):
        content = content.strip("` \n")
    if content.endswith("```"):
        content = content[:-3].strip()
    if content.lower().startswith("json"):
        content = content[4:].strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, flags=re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


async def search_tmdb(query: str, api_key: str, wanted_type: str | None = None) -> dict | None:
    if not api_key or not query:
        return None
    params = {
        "api_key": api_key,
        "query": query,
        "include_adult": "false",
        "language": "es-ES",
    }
    url = f"https://api.themoviedb.org/3/search/multi?{urllib.parse.urlencode(params)}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    for item in data.get("results", []):
                        if item.get("media_type") in {"movie", "tv"} and (not wanted_type or item.get("media_type") == wanted_type):
                            return item
    except Exception as e:
        logging.error(f"Error searching TMDB: {e}")
    return None


async def enrich_missing_from_tmdb(data: dict, config: Config) -> dict:
    if not config.tmdb_api_key or not data.get("name"):
        return data

    media_type = data.get("media_type")
    wanted_type = "tv" if media_type == "series" else "movie" if media_type == "movie" else None
    tmdb_data = await search_tmdb(str(data["name"]), config.tmdb_api_key, wanted_type)
    if not tmdb_data:
        return data

    enriched = dict(data)
    title = tmdb_data.get("title") or tmdb_data.get("name")
    date = tmdb_data.get("release_date") or tmdb_data.get("first_air_date") or ""

    if title and not enriched.get("name"):
        enriched["name"] = title
    if media_type == "movie" and date and not enriched.get("year"):
        enriched["year"] = date[:4]

    return enriched


async def parse_filename_with_llm(filename: str, caption: str, local_quality: str | None, config: Config) -> dict | None:
    if not config.llm_api_key or not config.llm_model:
        return None

    timeout = env_float("LLM_TIMEOUT", 15.0)
    client = AsyncOpenAI(
        api_key=config.llm_api_key,
        base_url=config.llm_base_url if config.llm_base_url else None,
        timeout=timeout,
        max_retries=1,
    )

    prompt = f"""
Analiza el siguiente nombre de archivo de video y/o su texto adjunto y extrae los metadatos de la película o serie.
Debes detectar si es una película o una serie, incluso si está mal etiquetado.

Reglas importantes:
- "t1 s3", "T1 E3", "Temp. 1 Ep.1", "Temporada 1 Episodio 1" significan temporada 1 y episodio 1/3 según corresponda.
- Siempre devuelve temporada como S con 2 dígitos: temporada 1 => "S01".
- Siempre devuelve episodio como E con 2 dígitos: episodio 1 => "E01".
- Si después del episodio aparece texto, normalmente es el título del episodio y debe ir en "optional". Ejemplo: "The Boys Temp. 1 Ep.1 Las reglas del juego" => optional "Las reglas del juego".
- Si ves calidad/resolución como 1080p, 720p, 2160p, etc. ponla en "quality".
- Si la calidad detectada localmente no es null, usa esa calidad salvo que el texto indique otra más clara.
- Si no hay calidad visible, usa null en "quality" y no inventes resolución.

Archivo: {filename}
Texto adjunto (si hay): {caption}
Calidad detectada localmente: {local_quality or "null"}

Devuelve ÚNICAMENTE un JSON válido con la siguiente estructura (omite los campos que no puedas detectar, pero respeta los nombres de las claves):

Para películas:
{{
  "media_type": "movie",
  "name": "Título limpio",
  "year": "Año (4 dígitos)",
  "quality": "Resolución (ej: 1080p, 720p)",
  "optional": "Opcionales (ej: WEBRip x265 Dual Audio)"
}}

Para series:
{{
  "media_type": "series",
  "name": "Título limpio",
  "season": "Temporada en formato S00 (ej: S01)",
  "episode": "Episodio en formato E00 (ej: E03)",
  "quality": "Resolución (ej: 1080p)",
  "optional": "Título del episodio u opcionales (ej: Las reglas del juego, WEB-DL DDP5.1)"
}}

Solo devuelve el JSON, sin texto adicional ni markdown.
"""
    try:
        if config.llm_debug:
            logging.info("LLM request model=%s base_url=%s filename=%r caption=%r", config.llm_model, config.llm_base_url or "default", filename, caption)
            logging.info("LLM prompt:\n%s", prompt)

        response = await client.chat.completions.create(
            model=config.llm_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=300
        )
        content = extract_llm_content(response)
        if config.llm_debug:
            logging.info("LLM response type: %s", type(response).__name__)
            logging.info("LLM raw response:\n%s", content)

        data = extract_json_object(content)
        if config.llm_debug:
            logging.info("LLM parsed JSON: %s", json.dumps(data, ensure_ascii=False))

        if data.get("media_type") in ("movie", "series"):
            return data
    except Exception as e:
        logging.error(f"Error parseando con LLM: {e}")
    return None


def is_multipart(file_name: str) -> bool:
    name_without_ext, _ = os.path.splitext(file_name)
    return bool(MULTIPART_RE.search(name_without_ext))


def detected_quality(video_info: dict, caption: str = "") -> str | None:
    searchable = f"{video_info.get('file_name') or ''} {caption}"
    if match := QUALITY_RE.search(searchable):
        return match.group(1).lower()

    height = video_info.get("height")
    if not height:
        return None

    if height >= 2000:
        return "2160p"
    if height >= 1300:
        return "1440p"
    if height >= 900:
        return "1080p"
    if height >= 650:
        return "720p"
    if height >= 450:
        return "480p"
    return f"{height}p"


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def clean_optional(value: str) -> str:
    value = clean_text(value)
    if value in {"-", ".", "no", "No", "NO", "ninguno", "Ninguno"}:
        return ""
    return value.strip(" .")


def normalize_quality(value: str) -> str:
    match = QUALITY_RE.search(value)
    return match.group(1).lower() if match else clean_text(value)


def ensure_extension(file_name: str) -> str:
    ext = get_extension(file_name)
    return ext if ext in VIDEO_EXTENSIONS else ".mkv"


def build_caption(data: dict) -> str:
    ext = ensure_extension(data["file_name"])
    optional = data.get("optional", "")

    if data["media_type"] == "movie":
        parts = [data["name"], data["year"], data["quality"]]
        if optional:
            parts.append(optional)
        return f"{' '.join(parts)}{ext}"

    parts = [f"{data['name']}.{data['season']}{data['episode']}"]
    if optional:
        parts.append(optional.replace(" ", "."))
    parts.append(data["quality"])
    return f"{'.'.join(parts)}{ext}"


def current_file_label(data: dict) -> str:
    return ""


def is_allowed(config: Config, user_id: int | None) -> bool:
    return bool(user_id) and (not config.allowed_user_ids or user_id in config.allowed_user_ids)


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


def command_args(message: Message) -> str:
    text = message.text or ""
    parts = text.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


async def reject_if_not_allowed(message: Message, config: Config) -> bool:
    if is_allowed(config, message.from_user.id if message.from_user else None):
        return False
    await safe_answer(message, "No tienes permiso para usar este bot.")
    return True


async def start(message: Message, state: FSMContext, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    await cleanup_temp_review_media(message.bot, state, message.chat.id)
    await state.clear()
    await safe_answer(
        message,
        "Envíame o reenvíame un video como archivo/documento o video. "
        "Puedes enviarme varios a la vez y los procesaré uno por uno en cola.\n"
        "No guardo archivos en disco; Telegram lo copia directo al canal."
    )


async def help_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    await safe_answer(
        message,
        "Comandos disponibles:\n"
        "/config - Ver configuración actual\n"
        "/autopost on|off - Activar/desactivar auto-publicación\n"
        "/debug on|off - Activar/desactivar logs del LLM\n"
        "/setchannel -1001234567890 o @canal - Cambiar canal destino hasta reiniciar\n"
        "/speed safe|normal|fast - Cambiar velocidad de cola\n"
        "/queue - Ver videos pendientes en cola\n"
        "/pending - Ver cuántos archivos requieren revisión\n"
        "/review - Revisar el siguiente archivo pendiente\n"
        "/clearqueue - Vaciar cola pendiente\n"
        "/cancel - Cancelar archivo actual y pasar al siguiente",
    )


async def config_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    user_id = message.from_user.id
    queue_len = len(user_queues.get(user_id, []))
    processing = "sí" if user_processing.get(user_id, False) else "no"
    await safe_answer(
        message,
        "Configuración actual:\n"
        f"Canal destino: <code>{config.target_channel_id}</code>\n"
        f"Auto-post: {'on' if config.llm_auto_post else 'off'}\n"
        f"LLM debug: {'on' if config.llm_debug else 'off'}\n"
        f"LLM configurado: {'sí' if config.llm_api_key and config.llm_model else 'no'}\n"
        f"TMDB configurado: {'sí' if config.tmdb_api_key else 'no'}\n"
        f"Intervalo Telegram: {config.telegram_min_interval}s\n"
        f"Pausa por archivo: {config.telegram_file_interval}s\n"
        f"Reintentos Telegram: {config.telegram_max_retries}\n"
        f"Aviso de cola cada: {config.queue_notify_every}\n"
        f"SQLite: <code>{config.database_path}</code>\n"
        f"Usuarios permitidos: {'todos' if not config.allowed_user_ids else len(config.allowed_user_ids)}\n"
        f"Procesando ahora: {processing}\n"
        f"Pendientes en cola: {queue_len}\n"
        f"Pendientes de revisión: {pending_review_count(config, user_id)}",
    )


async def autopost_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    args = command_args(message)
    if not args:
        await safe_answer(message, f"Auto-post está {'on' if config.llm_auto_post else 'off'}. Usa /autopost on o /autopost off.")
        return
    value = parse_bool_arg(args)
    if value is None:
        await safe_answer(message, "Valor inválido. Usa /autopost on o /autopost off.")
        return
    config.llm_auto_post = value
    save_setting(config, "llm_auto_post", "true" if value else "false")
    await safe_answer(message, f"Auto-post cambiado a {'on' if value else 'off'}.")


async def debug_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    args = command_args(message)
    if not args:
        await safe_answer(message, f"LLM debug está {'on' if config.llm_debug else 'off'}. Usa /debug on o /debug off.")
        return
    value = parse_bool_arg(args)
    if value is None:
        await safe_answer(message, "Valor inválido. Usa /debug on o /debug off.")
        return
    config.llm_debug = value
    save_setting(config, "llm_debug", "true" if value else "false")
    await safe_answer(message, f"LLM debug cambiado a {'on' if value else 'off'}.")


async def setchannel_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    target = parse_target_channel(command_args(message))
    if target is None:
        await safe_answer(message, "Uso: /setchannel -1001234567890 o /setchannel @nombre_del_canal")
        return
    config.target_channel_id = target
    save_setting(config, "target_channel_id", target)
    await safe_answer(message, f"Canal destino cambiado a <code>{target}</code> y guardado en SQLite.")


async def speed_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    args = command_args(message).lower()
    if not args:
        await safe_answer(message, f"Velocidad actual: min={config.telegram_min_interval}s, archivo={config.telegram_file_interval}s. Usa /speed safe, /speed normal o /speed fast.")
        return

    if args == "safe":
        config.telegram_min_interval = 2.0
        config.telegram_file_interval = 4.0
        config.queue_notify_every = 100
    elif args == "normal":
        config.telegram_min_interval = 1.2
        config.telegram_file_interval = 2.0
        config.queue_notify_every = 25
    elif args == "fast":
        config.telegram_min_interval = 0.6
        config.telegram_file_interval = 1.0
        config.queue_notify_every = 25
    else:
        await safe_answer(message, "Valor inválido. Usa /speed safe, /speed normal o /speed fast.")
        return

    save_setting(config, "telegram_min_interval", config.telegram_min_interval)
    save_setting(config, "telegram_file_interval", config.telegram_file_interval)
    save_setting(config, "queue_notify_every", config.queue_notify_every)
    await safe_answer(message, f"Velocidad cambiada a {args}: min={config.telegram_min_interval}s, archivo={config.telegram_file_interval}s, aviso cada {config.queue_notify_every}.")


async def queue_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    user_id = message.from_user.id
    queue = user_queues.get(user_id, [])
    if not queue:
        await safe_answer(message, "No hay videos pendientes en cola.")
        return
    names = []
    for index, queued_message in enumerate(queue[:10], start=1):
        info = get_video_info(queued_message)
        names.append(f"{index}. {info['file_name'] if info else 'archivo no válido'}")
    extra = "" if len(queue) <= 10 else f"\n...y {len(queue) - 10} más."
    await safe_answer(message, "Videos pendientes:\n" + "\n".join(names) + extra)


async def clearqueue_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    user_id = message.from_user.id
    removed = len(user_queues.get(user_id, []))
    user_queues[user_id] = []
    await safe_answer(message, f"Cola vaciada. Videos removidos: {removed}.")


async def pending_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    count = pending_review_count(config, message.from_user.id)
    await safe_answer(message, f"Archivos pendientes de revisión: {count}." + (" Usa /review para revisar el siguiente." if count else ""))


async def review_command(message: Message, state: FSMContext, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    row = get_next_pending_review(config, message.from_user.id)
    if not row:
        await safe_answer(message, "No hay archivos pendientes de revisión.")
        return

    video_info = video_info_from_pending(row)
    await state.clear()
    await state.update_data(**video_info, quality=row.get("quality"), pending_review_id=row["id"])
    await show_temp_review_media(message, state, message.bot, config, video_info)
    await state.set_state(MediaForm.choosing_type)
    await safe_answer(
        message,
        f"Revisando pendiente #{row['id']}\n"
        f"Motivo: {row.get('reason') or 'requiere asistencia'}\n\n"
        "¿Es película o serie?",
        reply_markup=type_keyboard(),
    )


def detected_quality_keyboard(quality: str | None, prefix: str) -> InlineKeyboardMarkup | None:
    if not quality:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Usar {quality}", callback_data=f"{prefix}:detected_quality")]
        ]
    )


def optional_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Sin extras", callback_data=f"{prefix}:none")]
        ]
    )


async def process_next_in_queue(user_id: int, state: FSMContext, bot: Bot, config: Config) -> None:
    queue = user_queues.get(user_id, [])
    if not queue:
        user_processing[user_id] = False
        processed = user_processed_counts.pop(user_id, 0)
        pending = pending_review_count(config, user_id)
        message = f"✅ Cola terminada. Archivos procesados: {processed}."
        if pending:
            message += f"\n⚠️ Pendientes para revisar: {pending}. Usa /review para revisarlos uno por uno."
        await safe_send_message(bot, user_id, message, config=config)
        return

    user_processing[user_id] = True
    user_processed_counts.setdefault(user_id, 0)
    message = queue.pop(0)
    
    # Rest of the receive_video logic goes here, but we pass the message
    await state.clear()
    
    video_info = get_video_info(message)
    if not video_info:
        await safe_answer(message, "Archivo omitido: no es un video válido .mkv o .mp4.", config=config)
        await after_file_processed(user_id, state, bot, config)
        return

    if is_multipart(video_info["file_name"]):
        await safe_answer(message, f"Archivo <b>{video_info['file_name']}</b> multipart detectado y omitido automáticamente.", config=config)
        await after_file_processed(user_id, state, bot, config)
        return

    ext = get_extension(video_info["file_name"])
    if ext and ext not in VIDEO_EXTENSIONS:
        await safe_answer(message, f"Archivo <b>{video_info['file_name']}</b> omitido: La extensión debe ser .mkv o .mp4.", config=config)
        await after_file_processed(user_id, state, bot, config)
        return

    if not config.llm_auto_post:
        await safe_answer(message, f"⏳ Procesando: <b>{video_info['file_name']}</b>...", config=config)

    caption_text = message.caption or ""
    quality = detected_quality(video_info, caption_text)
    await state.update_data(**video_info, quality=quality)

    local_data = local_parse_from_sources(video_info["file_name"], caption_text)
    if local_data:
        if quality:
            local_data["quality"] = quality
        if await handle_detected_media(user_id, message, state, bot, config, video_info, local_data):
            return

    # 1. Intentar con LLM primero si está configurado
    has_useful_context = bool(choose_search_query(video_info["file_name"], caption_text))
    if config.llm_api_key and config.llm_model and has_useful_context:
        llm_data = await parse_filename_with_llm(video_info["file_name"], caption_text, quality, config)
        
        if llm_data:
            current_data = await state.get_data()
            llm_data = normalize_llm_data(llm_data, quality, video_info["file_name"], caption_text)
                
            merged_data = {**current_data, **llm_data}
            merged_data = await enrich_missing_from_tmdb(merged_data, config)
            await state.update_data(**merged_data)

            missing = required_fields_missing(merged_data)
            if missing:
                logging.info("LLM detected partial metadata, missing fields: %s", ", ".join(missing))
                if config.llm_auto_post:
                    pending_id = save_pending_review(config, user_id, video_info, caption_text, merged_data.get("quality"), f"missing: {', '.join(missing)}")
                    logging.info("Saved pending review #%s for %s", pending_id, video_info.get("file_name"))
                    await state.clear()
                    await after_file_processed(user_id, state, bot, config)
                    return
                if await ask_missing_required_fields(message, state, bot, config, merged_data, missing):
                    return

            try:
                caption = build_caption(merged_data)
                await state.update_data(caption=caption)
                
                if config.llm_auto_post:
                    try:
                        published = await publish_media(bot, config, merged_data, caption)
                        if not published:
                            await safe_answer(message, f"⏭️ Duplicado omitido: <code>{caption}</code>", config=config)
                        
                        await state.clear()
                        await after_file_processed(user_id, state, bot, config)
                        return
                    except (TelegramBadRequest, TelegramForbiddenError, TimeoutError) as exc:
                        await safe_answer(message, f"❌ Error enviando auto-post al canal: {exc}\nPasando a modo manual.", config=config)
                        # Si falla el auto-post, cae al flujo manual normal
                
                await state.set_state(MediaForm.confirming_llm)
                
                text = f"🤖 <b>Detección inteligente (LLM)</b>\n\nHe generado el siguiente formato:\n<code>{caption}</code>\n\n¿Deseas enviarlo directamente o editarlo manualmente?"
                await safe_answer(message, text, reply_markup=llm_confirm_keyboard())
                return
            except KeyError as e:
                logging.error(f"LLM data missing key for build_caption: {e}")

    # 2. Intentar con TMDB si LLM no está o falló
    if config.tmdb_api_key:
        search_query = choose_search_query(video_info["file_name"], caption_text)
        if search_query:
            tmdb_data = await search_tmdb(search_query, config.tmdb_api_key)
            if tmdb_data:
                await state.set_state(MediaForm.confirming_tmdb)
                await state.update_data(tmdb_data=tmdb_data)
                
                media_type = tmdb_data.get("media_type", "Desconocido")
                title = tmdb_data.get("title") or tmdb_data.get("name") or "Desconocido"
                date = tmdb_data.get("release_date") or tmdb_data.get("first_air_date") or ""
                year = date[:4] if date else "Desconocido"
                kind = "Serie" if media_type == "tv" else "Película"
                
                text = f"Encontré esto en TMDB:\n\n<b>{title}</b> ({year}) - {kind}\n\n¿Es correcto?"
                await safe_answer(message, text, reply_markup=tmdb_confirm_keyboard())
                return

    if config.llm_auto_post:
        pending_id = save_pending_review(config, user_id, video_info, caption_text, quality, "insufficient metadata")
        logging.info("Saved pending review #%s for %s", pending_id, video_info.get("file_name"))
        await state.clear()
        await after_file_processed(user_id, state, bot, config)
        return

    await state.set_state(MediaForm.choosing_type)
    quality_text = f" Detecté calidad: {quality}." if quality else " No pude detectar la calidad."
    await safe_answer(message, f"¿Es película o serie?{quality_text}", reply_markup=type_keyboard())


async def receive_video(message: Message, state: FSMContext, bot: Bot, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return

    user_id = message.from_user.id
    if user_id not in user_queues:
        user_queues[user_id] = []
        
    user_queues[user_id].append(message)
    
    queue_length = len(user_queues[user_id])
    notify_every = max(1, config.queue_notify_every)
    if queue_length in {2, 10} or (queue_length > 10 and queue_length % notify_every == 0):
        await safe_answer(message, f"📥 Videos pendientes en cola: {queue_length}.", config=config)
        
    if not user_processing.get(user_id, False):
        await process_next_in_queue(user_id, state, bot, config)


async def handle_llm_confirm(callback: CallbackQuery, state: FSMContext, bot: Bot, config: Config) -> None:
    if not is_allowed(config, callback.from_user.id if callback.from_user else None):
        await callback.answer("No autorizado", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]
    
    if action == "send":
        await callback.answer()
        data = await state.get_data()
        try:
            published = await publish_media(bot, config, data, data["caption"])
            
            pending_id = data.get("pending_review_id")
            if pending_id:
                mark_pending_review_done(config, int(pending_id))
            await cleanup_temp_review_media(bot, state, callback.from_user.id)
            await state.clear()
            await safe_answer(callback.message, "✅ Archivo enviado al canal correctamente." if published else "⏭️ Duplicado omitido; ya estaba publicado.", config=config)
        except (TelegramBadRequest, TelegramForbiddenError, TimeoutError) as exc:
            await safe_answer(callback.message, f"Error enviando al canal: {exc}", config=config)
            
        await after_file_processed(callback.from_user.id, state, bot, config)
        return

    # Si elige editar manual, vamos a la selección de tipo
    await callback.answer()
    data = await state.get_data()
    await show_temp_review_media(callback.message, state, bot, config, data)
    quality = data.get("quality")
    quality_text = f" Detecté calidad: {quality}." if quality else " No pude detectar la calidad."
    
    await state.set_state(MediaForm.choosing_type)
    await safe_answer(callback.message, f"Modo manual. ¿Es película o serie?{quality_text}", reply_markup=type_keyboard())


async def handle_tmdb_confirm(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_allowed(config, callback.from_user.id if callback.from_user else None):
        await callback.answer("No autorizado", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]
    await callback.answer()
    
    data = await state.get_data()
    await show_temp_review_media(callback.message, state, callback.bot, config, data)
    tmdb_data = data.get("tmdb_data", {})
    quality = data.get("quality")
    quality_text = f" Detecté calidad: {quality}." if quality else " No pude detectar la calidad."

    if action == "no":
        await state.set_state(MediaForm.choosing_type)
        await safe_answer(callback.message, f"Ok, continuemos manual. ¿Es película o serie?{quality_text}", reply_markup=type_keyboard())
        return

    media_type = "series" if tmdb_data.get("media_type") == "tv" else "movie"
    title = tmdb_data.get("title") or tmdb_data.get("name") or ""
    date = tmdb_data.get("release_date") or tmdb_data.get("first_air_date") or ""
    await state.update_data(
        media_type=media_type,
        name=title,
        year=date[:4]
    )

    if media_type == "movie":
        await state.set_state(MediaForm.movie_quality)
        await ask_movie_quality(callback.message, state, f"Película: {title} ({date[:4]})\n\n")
    else:
        await state.set_state(MediaForm.series_season)
        await safe_answer(callback.message, f"Serie: {title}\n\nTemporada con S y mínimo 2 dígitos. Ejemplo: S01")


async def choose_type(callback: CallbackQuery, state: FSMContext, config: Config) -> None:
    if not is_allowed(config, callback.from_user.id if callback.from_user else None):
        await callback.answer("No autorizado", show_alert=True)
        return

    media_type = callback.data.split(":", 1)[1]
    await callback.answer()
    await state.update_data(media_type=media_type)
    data = await state.get_data()
    await show_temp_review_media(callback.message, state, callback.bot, config, data)

    if media_type == "movie":
        await state.set_state(MediaForm.movie_name)
        await safe_answer(callback.message, current_file_label(data) + "Título de la película. Ejemplo: Ghosted")
        return

    await state.set_state(MediaForm.series_name)
    await safe_answer(callback.message, current_file_label(data) + "Título de la serie. Ejemplo: Harikatha Sambhavami Yuge Yuge")


async def movie_name(message: Message, state: FSMContext) -> None:
    await state.update_data(name=clean_text(message.text or ""))
    await state.set_state(MediaForm.movie_year)
    data = await state.get_data()
    await safe_answer(message, current_file_label(data) + "Año de estreno. Ejemplo: 2023")


async def ask_movie_quality(message: Message, state: FSMContext, prefix: str = "") -> None:
    data = await state.get_data()
    detected = data.get("quality")
    text = current_file_label(data) + f"{prefix}Calidad o resolución. Ejemplo: 1080p"
    if detected:
        text += f"\nDetectada: {detected}. Puedes tocar el botón o escribir otra."
    await safe_answer(message, text, reply_markup=detected_quality_keyboard(detected, "movie_quality"))


async def ask_movie_optional(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    await safe_answer(
        message,
        current_file_label(data) + "Opcionales: codec, audio, fuente. Ejemplo: WEBRip x265 Dual Audio\nSi no hay extras, toca el botón.",
        reply_markup=optional_keyboard("movie_optional"),
    )


async def movie_year(message: Message, state: FSMContext) -> None:
    year = clean_text(message.text or "")
    if not re.fullmatch(r"\d{4}", year):
        await safe_answer(message, "El año debe tener 4 números. Ejemplo: 2023")
        return
    await state.update_data(year=year)
    await state.set_state(MediaForm.movie_quality)
    await ask_movie_quality(message, state)


async def movie_quality(message: Message, state: FSMContext) -> None:
    await state.update_data(quality=normalize_quality(message.text or ""))
    await state.set_state(MediaForm.movie_optional)
    await ask_movie_optional(message, state)


async def movie_optional(message: Message, state: FSMContext) -> None:
    await state.update_data(optional=clean_optional(message.text or ""))
    await show_preview(message, state)


async def series_name(message: Message, state: FSMContext) -> None:
    await state.update_data(name=clean_text(message.text or ""))
    await state.set_state(MediaForm.series_season)
    data = await state.get_data()
    await safe_answer(message, current_file_label(data) + "Temporada con S y mínimo 2 dígitos. Ejemplo: S01")


async def ask_series_quality(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    detected = data.get("quality")
    text = current_file_label(data) + "Calidad o resolución. Ejemplo: 1080p"
    if detected:
        text += f"\nDetectada: {detected}. Puedes tocar el botón o escribir otra."
    await safe_answer(message, text, reply_markup=detected_quality_keyboard(detected, "series_quality"))


async def ask_series_optional(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    await safe_answer(
        message,
        current_file_label(data) + "Opcionales: título del episodio, codec, audio, fuente. Ejemplo: WEB-DL DDP5.1\nSi no hay extras, toca el botón.",
        reply_markup=optional_keyboard("series_optional"),
    )


async def series_season(message: Message, state: FSMContext) -> None:
    season = clean_text(message.text or "").upper()
    if not SEASON_RE.fullmatch(season):
        await safe_answer(message, "Formato inválido. Usa S seguido de mínimo 2 números. Ejemplo: S01")
        return
    await state.update_data(season=season)
    await state.set_state(MediaForm.series_episode)
    data = await state.get_data()
    await safe_answer(message, current_file_label(data) + "Episodio con E y mínimo 2 dígitos. Ejemplo: E04")


async def series_episode(message: Message, state: FSMContext) -> None:
    episode = clean_text(message.text or "").upper()
    if not EPISODE_RE.fullmatch(episode):
        await safe_answer(message, "Formato inválido. Usa E seguido de mínimo 2 números. Ejemplo: E04")
        return
    await state.update_data(episode=episode)
    await state.set_state(MediaForm.series_quality)
    await ask_series_quality(message, state)


async def series_quality(message: Message, state: FSMContext) -> None:
    await state.update_data(quality=normalize_quality(message.text or ""))
    await state.set_state(MediaForm.series_optional)
    await ask_series_optional(message, state)


async def series_optional(message: Message, state: FSMContext) -> None:
    await state.update_data(optional=clean_optional(message.text or ""))
    await show_preview(message, state)


async def handle_movie_quality_callback(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    quality = data.get("quality")
    if not quality:
        await callback.answer("No hay calidad detectada", show_alert=True)
        return
    await callback.answer()
    await state.update_data(quality=quality)
    await state.set_state(MediaForm.movie_optional)
    await ask_movie_optional(callback.message, state)


async def handle_series_quality_callback(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    quality = data.get("quality")
    if not quality:
        await callback.answer("No hay calidad detectada", show_alert=True)
        return
    await callback.answer()
    await state.update_data(quality=quality)
    await state.set_state(MediaForm.series_optional)
    await ask_series_optional(callback.message, state)


async def handle_movie_optional_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.update_data(optional="")
    await show_preview(callback.message, state)


async def handle_series_optional_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.update_data(optional="")
    await show_preview(callback.message, state)


async def show_preview(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    caption = build_caption(data)
    await state.update_data(caption=caption)
    await state.set_state(MediaForm.confirming)
    await safe_answer(message, f"Vista previa:\n<code>{caption}</code>", reply_markup=confirm_keyboard())


async def confirm(callback: CallbackQuery, state: FSMContext, bot: Bot, config: Config) -> None:
    if not is_allowed(config, callback.from_user.id if callback.from_user else None):
        await callback.answer("No autorizado", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]
    if action == "cancel":
        await callback.answer("Cancelado")
        await safe_answer(callback.message, "Operación cancelada para este archivo.", config=config)
        await after_file_processed(callback.from_user.id, state, bot, config)
        return

    data = await state.get_data()
    try:
        published = await publish_media(bot, config, data, data["caption"])
    except (TelegramBadRequest, TelegramForbiddenError, TimeoutError) as exc:
        await callback.answer("No se pudo enviar", show_alert=True)
        await safe_answer(callback.message, f"Error enviando al canal: {exc}", config=config)
        await after_file_processed(callback.from_user.id, state, bot, config)
        return

    await callback.answer("Enviado" if published else "Duplicado")
    await safe_answer(callback.message, "✅ Archivo enviado al canal." if published else "⏭️ Duplicado omitido; ya estaba publicado.", config=config)
    pending_id = data.get("pending_review_id")
    if pending_id:
        mark_pending_review_done(config, int(pending_id))
    await after_file_processed(callback.from_user.id, state, bot, config)


async def cancel(message: Message, state: FSMContext, bot: Bot, config: Config) -> None:
    await safe_answer(message, "Operación cancelada para este archivo.", config=config)
    await after_file_processed(message.from_user.id, state, bot, config)


async def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    config = load_config()
    init_database(config)
    apply_persisted_settings(config)
    bot = Bot(config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    async def start_handler(message: Message, state: FSMContext) -> None:
        await start(message, state, config)

    async def help_handler(message: Message) -> None:
        await help_command(message, config)

    async def config_handler(message: Message) -> None:
        await config_command(message, config)

    async def autopost_handler(message: Message) -> None:
        await autopost_command(message, config)

    async def debug_handler(message: Message) -> None:
        await debug_command(message, config)

    async def setchannel_handler(message: Message) -> None:
        await setchannel_command(message, config)

    async def speed_handler(message: Message) -> None:
        await speed_command(message, config)

    async def queue_handler(message: Message) -> None:
        await queue_command(message, config)

    async def clearqueue_handler(message: Message) -> None:
        await clearqueue_command(message, config)

    async def pending_handler(message: Message) -> None:
        await pending_command(message, config)

    async def review_handler(message: Message, state: FSMContext) -> None:
        await review_command(message, state, config)

    async def cancel_handler(message: Message, state: FSMContext) -> None:
        await cancel(message, state, bot, config)

    async def receive_video_handler(message: Message, state: FSMContext) -> None:
        await receive_video(message, state, bot, config)

    async def llm_confirm_handler(callback: CallbackQuery, state: FSMContext) -> None:
        await handle_llm_confirm(callback, state, bot, config)

    async def tmdb_confirm_handler(callback: CallbackQuery, state: FSMContext) -> None:
        await handle_tmdb_confirm(callback, state, config)

    async def choose_type_handler(callback: CallbackQuery, state: FSMContext) -> None:
        await choose_type(callback, state, config)

    async def confirm_handler(callback: CallbackQuery, state: FSMContext) -> None:
        await confirm(callback, state, bot, config)

    async def movie_quality_callback_handler(callback: CallbackQuery, state: FSMContext) -> None:
        await handle_movie_quality_callback(callback, state)

    async def series_quality_callback_handler(callback: CallbackQuery, state: FSMContext) -> None:
        await handle_series_quality_callback(callback, state)

    async def movie_optional_callback_handler(callback: CallbackQuery, state: FSMContext) -> None:
        await handle_movie_optional_callback(callback, state)

    async def series_optional_callback_handler(callback: CallbackQuery, state: FSMContext) -> None:
        await handle_series_optional_callback(callback, state)

    private_chat = F.chat.type == "private"

    dp.message.register(start_handler, CommandStart(), private_chat)
    dp.message.register(help_handler, Command("help"), private_chat)
    dp.message.register(config_handler, Command("config"), private_chat)
    dp.message.register(autopost_handler, Command("autopost"), private_chat)
    dp.message.register(debug_handler, Command("debug"), private_chat)
    dp.message.register(setchannel_handler, Command("setchannel"), private_chat)
    dp.message.register(speed_handler, Command("speed"), private_chat)
    dp.message.register(queue_handler, Command("queue"), private_chat)
    dp.message.register(pending_handler, Command("pending"), private_chat)
    dp.message.register(review_handler, Command("review"), private_chat)
    dp.message.register(clearqueue_handler, Command("clearqueue"), private_chat)
    dp.message.register(cancel_handler, F.text.casefold() == "/cancel", private_chat)
    dp.message.register(receive_video_handler, (F.video | F.document), private_chat)
    dp.callback_query.register(llm_confirm_handler, F.data.startswith("llm:"), MediaForm.confirming_llm)
    dp.callback_query.register(tmdb_confirm_handler, F.data.startswith("tmdb:"), MediaForm.confirming_tmdb)
    dp.callback_query.register(choose_type_handler, F.data.startswith("type:"), MediaForm.choosing_type)
    dp.callback_query.register(movie_quality_callback_handler, F.data == "movie_quality:detected_quality", MediaForm.movie_quality)
    dp.callback_query.register(series_quality_callback_handler, F.data == "series_quality:detected_quality", MediaForm.series_quality)
    dp.callback_query.register(movie_optional_callback_handler, F.data == "movie_optional:none", MediaForm.movie_optional)
    dp.callback_query.register(series_optional_callback_handler, F.data == "series_optional:none", MediaForm.series_optional)
    dp.message.register(movie_name, MediaForm.movie_name, private_chat)
    dp.message.register(movie_year, MediaForm.movie_year, private_chat)
    dp.message.register(movie_quality, MediaForm.movie_quality, private_chat)
    dp.message.register(movie_optional, MediaForm.movie_optional, private_chat)
    dp.message.register(series_name, MediaForm.series_name, private_chat)
    dp.message.register(series_season, MediaForm.series_season, private_chat)
    dp.message.register(series_episode, MediaForm.series_episode, private_chat)
    dp.message.register(series_quality, MediaForm.series_quality, private_chat)
    dp.message.register(series_optional, MediaForm.series_optional, private_chat)
    dp.callback_query.register(confirm_handler, F.data.startswith("confirm:"), MediaForm.confirming)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
