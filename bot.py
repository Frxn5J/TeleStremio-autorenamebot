from __future__ import annotations

import asyncio
import json
import logging
import os
import re
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


async def after_file_processed(user_id: int, state: FSMContext, bot: Bot, config: "Config") -> None:
    user_processed_counts[user_id] = user_processed_counts.get(user_id, 0) + 1
    await asyncio.sleep(config.telegram_file_interval)
    await process_next_in_queue(user_id, state, bot, config)

VIDEO_EXTENSIONS = {".mkv", ".mp4"}
MULTIPART_RE = re.compile(r"(?:^|[\s._-])(?:part\s*\d+|cd\s*\d+)(?:[\s._-]|$)", re.IGNORECASE)
SEASON_RE = re.compile(r"^S\d{2,}$", re.IGNORECASE)
EPISODE_RE = re.compile(r"^E\d{2,}$", re.IGNORECASE)
QUALITY_RE = re.compile(r"(2160p|1440p|1080p|720p|576p|540p|480p|360p)", re.IGNORECASE)
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


def normalize_llm_data(data: dict, fallback_quality: str | None, combined_text: str) -> dict:
    result = dict(data)
    local_series = local_parse_series(combined_text)
    if local_series and result.get("media_type") == "series":
        for key in ("name", "season", "episode"):
            if not result.get(key):
                result[key] = local_series.get(key)
        if not result.get("optional"):
            result["optional"] = local_series.get("optional", "")

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


async def search_tmdb(query: str, api_key: str) -> dict | None:
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
                        if item.get("media_type") in {"movie", "tv"}:
                            return item
    except Exception as e:
        logging.error(f"Error searching TMDB: {e}")
    return None


async def parse_filename_with_llm(filename: str, caption: str, local_quality: str | None, config: Config) -> dict | None:
    if not config.llm_api_key or not config.llm_model:
        return None

    client = AsyncOpenAI(
        api_key=config.llm_api_key,
        base_url=config.llm_base_url if config.llm_base_url else None
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
        f"Usuarios permitidos: {'todos' if not config.allowed_user_ids else len(config.allowed_user_ids)}\n"
        f"Procesando ahora: {processing}\n"
        f"Pendientes en cola: {queue_len}",
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
    await safe_answer(message, f"LLM debug cambiado a {'on' if value else 'off'}.")


async def setchannel_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    target = parse_target_channel(command_args(message))
    if target is None:
        await safe_answer(message, "Uso: /setchannel -1001234567890 o /setchannel @nombre_del_canal")
        return
    config.target_channel_id = target
    await safe_answer(message, f"Canal destino cambiado a <code>{target}</code>. Este cambio dura hasta reiniciar el bot.")


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
        await safe_send_message(bot, user_id, f"✅ Cola terminada. Archivos procesados: {processed}.", config=config)
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
    combined_text = f"{video_info['file_name']} {caption_text}"
    quality = detected_quality(video_info, caption_text)
    await state.update_data(**video_info, quality=quality)

    # 1. Intentar con LLM primero si está configurado
    if config.llm_api_key and config.llm_model:
        llm_data = await parse_filename_with_llm(video_info["file_name"], caption_text, quality, config)
        
        if llm_data:
            current_data = await state.get_data()
            llm_data = normalize_llm_data(llm_data, quality, combined_text)
                
            merged_data = {**current_data, **llm_data}
            await state.update_data(**merged_data)

            missing = required_fields_missing(merged_data)
            if missing:
                logging.info("LLM detected partial metadata, missing fields: %s", ", ".join(missing))
                if merged_data.get("media_type") == "series" and missing == ["quality"]:
                    await state.set_state(MediaForm.series_quality)
                    await ask_series_quality(message, state)
                    return
                if merged_data.get("media_type") == "movie" and missing == ["quality"]:
                    await state.set_state(MediaForm.movie_quality)
                    await ask_movie_quality(message, state)
                    return

            try:
                caption = build_caption(merged_data)
                await state.update_data(caption=caption)
                
                if config.llm_auto_post:
                    try:
                        await safe_send_media(bot, config.target_channel_id, video_info, caption, config=config)
                        
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

    local_data = local_parse_series(combined_text)
    if local_data:
        if quality:
            local_data["quality"] = quality
        await state.update_data(**local_data)
        if local_data.get("quality"):
            merged_data = {**(await state.get_data()), **local_data}
            missing = required_fields_missing(merged_data)
            if missing:
                logging.info("LLM detected partial metadata, missing fields: %s", ", ".join(missing))
                if merged_data.get("media_type") == "series" and missing == ["quality"]:
                    await state.set_state(MediaForm.series_quality)
                    await ask_series_quality(message, state)
                    return
                if merged_data.get("media_type") == "movie" and missing == ["quality"]:
                    await state.set_state(MediaForm.movie_quality)
                    await ask_movie_quality(message, state)
                    return

            try:
                caption = build_caption(merged_data)
                await state.update_data(caption=caption)
                if config.llm_auto_post:
                    try:
                        await safe_send_media(bot, config.target_channel_id, video_info, caption, config=config)
                        await state.clear()
                        await after_file_processed(user_id, state, bot, config)
                        return
                    except (TelegramBadRequest, TelegramForbiddenError, TimeoutError) as exc:
                        await safe_answer(message, f"❌ Error enviando auto-post al canal: {exc}\nPasando a modo manual.", config=config)
                await state.set_state(MediaForm.confirming_llm)
                await safe_answer(message, f"Detección local:\n<code>{caption}</code>\n\n¿Deseas enviarlo directamente o editarlo manualmente?", reply_markup=llm_confirm_keyboard())
                return
            except KeyError as e:
                logging.error(f"Local data missing key for build_caption: {e}")
        await state.set_state(MediaForm.series_quality)
        await ask_series_quality(message, state)
        return

    # 2. Intentar con TMDB si LLM no está o falló
    if config.tmdb_api_key:
        search_query = clean_filename_for_search(video_info["file_name"])
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
            await safe_send_media(bot, config.target_channel_id, data, data["caption"], config=config)
            
            await state.clear()
            await safe_answer(callback.message, "✅ Archivo enviado al canal correctamente.", config=config)
        except (TelegramBadRequest, TelegramForbiddenError, TimeoutError) as exc:
            await safe_answer(callback.message, f"Error enviando al canal: {exc}", config=config)
            
        await after_file_processed(callback.from_user.id, state, bot, config)
        return

    # Si elige editar manual, vamos a la selección de tipo
    await callback.answer()
    data = await state.get_data()
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

    if media_type == "movie":
        await state.set_state(MediaForm.movie_name)
        await safe_answer(callback.message, "Título de la película. Ejemplo: Ghosted")
        return

    await state.set_state(MediaForm.series_name)
    await safe_answer(callback.message, "Título de la serie. Ejemplo: Harikatha Sambhavami Yuge Yuge")


async def movie_name(message: Message, state: FSMContext) -> None:
    await state.update_data(name=clean_text(message.text or ""))
    await state.set_state(MediaForm.movie_year)
    await safe_answer(message, "Año de estreno. Ejemplo: 2023")


async def ask_movie_quality(message: Message, state: FSMContext, prefix: str = "") -> None:
    data = await state.get_data()
    detected = data.get("quality")
    text = f"{prefix}Calidad o resolución. Ejemplo: 1080p"
    if detected:
        text += f"\nDetectada: {detected}. Puedes tocar el botón o escribir otra."
    await safe_answer(message, text, reply_markup=detected_quality_keyboard(detected, "movie_quality"))


async def ask_movie_optional(message: Message) -> None:
    await safe_answer(
        message,
        "Opcionales: codec, audio, fuente. Ejemplo: WEBRip x265 Dual Audio\nSi no hay extras, toca el botón.",
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
    await ask_movie_optional(message)


async def movie_optional(message: Message, state: FSMContext) -> None:
    await state.update_data(optional=clean_optional(message.text or ""))
    await show_preview(message, state)


async def series_name(message: Message, state: FSMContext) -> None:
    await state.update_data(name=clean_text(message.text or ""))
    await state.set_state(MediaForm.series_season)
    await safe_answer(message, "Temporada con S y mínimo 2 dígitos. Ejemplo: S01")


async def ask_series_quality(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    detected = data.get("quality")
    text = "Calidad o resolución. Ejemplo: 1080p"
    if detected:
        text += f"\nDetectada: {detected}. Puedes tocar el botón o escribir otra."
    await safe_answer(message, text, reply_markup=detected_quality_keyboard(detected, "series_quality"))


async def ask_series_optional(message: Message) -> None:
    await safe_answer(
        message,
        "Opcionales: título del episodio, codec, audio, fuente. Ejemplo: WEB-DL DDP5.1\nSi no hay extras, toca el botón.",
        reply_markup=optional_keyboard("series_optional"),
    )


async def series_season(message: Message, state: FSMContext) -> None:
    season = clean_text(message.text or "").upper()
    if not SEASON_RE.fullmatch(season):
        await safe_answer(message, "Formato inválido. Usa S seguido de mínimo 2 números. Ejemplo: S01")
        return
    await state.update_data(season=season)
    await state.set_state(MediaForm.series_episode)
    await safe_answer(message, "Episodio con E y mínimo 2 dígitos. Ejemplo: E04")


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
    await ask_series_optional(message)


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
    await ask_movie_optional(callback.message)


async def handle_series_quality_callback(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    quality = data.get("quality")
    if not quality:
        await callback.answer("No hay calidad detectada", show_alert=True)
        return
    await callback.answer()
    await state.update_data(quality=quality)
    await state.set_state(MediaForm.series_optional)
    await ask_series_optional(callback.message)


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
        await safe_send_media(bot, config.target_channel_id, data, data["caption"], config=config)
    except (TelegramBadRequest, TelegramForbiddenError, TimeoutError) as exc:
        await callback.answer("No se pudo enviar", show_alert=True)
        await safe_answer(callback.message, f"Error enviando al canal: {exc}", config=config)
        await after_file_processed(callback.from_user.id, state, bot, config)
        return

    await callback.answer("Enviado")
    await safe_answer(callback.message, "✅ Archivo enviado al canal.", config=config)
    await after_file_processed(callback.from_user.id, state, bot, config)


async def cancel(message: Message, state: FSMContext, bot: Bot, config: Config) -> None:
    await safe_answer(message, "Operación cancelada para este archivo.", config=config)
    await after_file_processed(message.from_user.id, state, bot, config)


async def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    config = load_config()
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
