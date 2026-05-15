from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, Message

from app.config import Config, env_int, load_config, parse_bool_arg, parse_target_channel
from app.database import (
    already_published,
    apply_persisted_settings,
    get_next_pending_review,
    init_database,
    mark_pending_review_done,
    pending_review_count,
    register_published,
    reset_database,
    save_pending_review,
    save_setting,
)
from app.keyboards import (
    confirm_keyboard,
    detected_quality_keyboard,
    llm_confirm_keyboard,
    optional_keyboard,
    tmdb_confirm_keyboard,
    type_keyboard,
)
from app.llm import parse_filename_with_llm
from app.metadata import (
    EPISODE_RE,
    SEASON_RE,
    VIDEO_EXTENSIONS,
    build_caption,
    choose_search_query,
    clean_detected_name,
    clean_optional,
    clean_text,
    current_file_label,
    detected_quality,
    ffprobe_tags_to_text,
    get_extension,
    get_video_info,
    is_multipart,
    local_parse_from_sources,
    local_parse_from_ffprobe,
    normalize_llm_data,
    normalize_quality,
    required_fields_missing,
    strip_noise_text,
    tmdb_result_to_metadata,
    video_info_from_pending,
)
from app.states import MediaForm
from app.tmdb import enrich_missing_from_tmdb, search_tmdb

# Diccionario global para manejar las colas por usuario
user_queues: dict[int, list[Message]] = {}
user_processing: dict[int, bool] = {}
user_processed_counts: dict[int, int] = {}
telegram_send_lock = asyncio.Lock()
deep_scan_lock = asyncio.Lock()
last_telegram_send_at = 0.0
DEEP_SCAN_FILE_TOO_BIG = "__telegram_file_too_big__"



async def publish_media(bot: Bot, config: "Config", data: dict, caption: str) -> bool:
    if already_published(config, caption):
        logging.info("Duplicate skipped: %s", caption)
        return False
    await safe_send_media(bot, config.target_channel_id, data, caption, config=config)
    register_published(config, data, caption)
    return True


async def save_unresolved_review(message: Message, user_id: int, config: Config, video_info: dict, quality: str | None, reason: str) -> None:
    pending_id = save_pending_review(config, user_id, video_info, message.caption or "", quality, reason)
    logging.info("Saved pending review #%s for %s", pending_id, video_info.get("file_name"))
    await safe_answer(message, f"⚠️ No pude detectar metadata suficiente. Guardado como pendiente #{pending_id}. Usa /review para revisarlo.", config=config)


async def run_ffprobe(file_path: str, timeout: float) -> dict | None:
    executable = shutil.which("ffprobe")
    if not executable:
        return None

    process = await asyncio.create_subprocess_exec(
        executable,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        file_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        await process.communicate()
        logging.warning("ffprobe timed out for %s", file_path)
        return None

    if process.returncode != 0 or not stdout:
        return None
    try:
        return json.loads(stdout.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as exc:
        logging.warning("Could not parse ffprobe output: %s", exc)
        return None


async def download_and_probe(bot: Bot, config: Config, video_info: dict) -> dict | None:
    file_size = video_info.get("file_size") or 0
    max_bytes = max(1, config.deep_scan_max_mb) * 1024 * 1024
    if file_size and file_size > max_bytes:
        logging.info("Skipping deep scan for %s: size %s exceeds %s", video_info.get("file_name"), file_size, max_bytes)
        return None

    temp_dir = tempfile.mkdtemp(prefix="telestremio-deep-")
    temp_path = os.path.join(temp_dir, "input" + (get_extension(video_info.get("file_name") or "") or ".mkv"))
    try:
        telegram_file = await bot.get_file(video_info["file_id"])
        if not telegram_file.file_path:
            return None
        await bot.download_file(telegram_file.file_path, destination=temp_path)
        return await run_ffprobe(temp_path, config.deep_scan_timeout)
    except Exception as exc:
        if "file is too big" in str(exc).lower():
            logging.info("Deep scan skipped for %s: Telegram file is too big", video_info.get("file_name"))
            return {DEEP_SCAN_FILE_TOO_BIG: True}
        logging.warning("Deep scan download/probe failed for %s: %s", video_info.get("file_name"), exc)
        return None
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def handle_deep_scan_fallback(user_id: int, message: Message, state: FSMContext, bot: Bot, config: Config, video_info: dict, quality: str | None) -> bool:
    if not config.deep_scan_enabled:
        return False

    caption_text = message.caption or ""

    async def try_detected(detected_data: dict | None) -> bool:
        if not detected_data:
            return False
        enriched = await enrich_missing_from_tmdb(detected_data, config)
        if required_fields_missing(enriched):
            return False
        return await handle_detected_media(user_id, message, state, bot, config, video_info, enriched)

    text_quality = quality or detected_quality(video_info, caption_text)
    search_query = choose_search_query(video_info["file_name"], caption_text)
    if config.tmdb_api_key and search_query:
        tmdb_data = await search_tmdb(search_query, config.tmdb_api_key)
        if tmdb_data:
            tmdb_detected = tmdb_result_to_metadata(tmdb_data, text_quality, video_info["file_name"], caption_text)
            if await try_detected(tmdb_detected):
                return True

    local_data = local_parse_from_sources(video_info["file_name"], caption_text)
    if local_data:
        if text_quality and not local_data.get("quality"):
            local_data["quality"] = text_quality
        if await try_detected(local_data):
            return True

    sanitized_file_name = strip_noise_text(video_info["file_name"])
    sanitized_caption = strip_noise_text(caption_text)
    if sanitized_file_name != video_info["file_name"] or sanitized_caption != caption_text:
        search_query = choose_search_query(sanitized_file_name, sanitized_caption)
        if config.tmdb_api_key and search_query:
            tmdb_data = await search_tmdb(search_query, config.tmdb_api_key)
            if tmdb_data:
                tmdb_detected = tmdb_result_to_metadata(tmdb_data, text_quality, sanitized_file_name, sanitized_caption)
                if await try_detected(tmdb_detected):
                    return True

        local_data = local_parse_from_sources(sanitized_file_name, sanitized_caption)
        if local_data:
            if text_quality and not local_data.get("quality"):
                local_data["quality"] = text_quality
            if await try_detected(local_data):
                return True

    async with deep_scan_lock:
        await safe_answer(message, f"🔎 Detección normal falló. Analizando temporalmente: <b>{video_info['file_name']}</b>...", config=config)
        probe = await download_and_probe(bot, config, video_info)

    probe_text = ffprobe_tags_to_text(probe)
    combined_name = clean_text(" ".join(part for part in [video_info["file_name"], probe_text] if part))
    scan_quality = quality or detected_quality({**video_info, "file_name": combined_name}, caption_text)

    if probe and probe.get(DEEP_SCAN_FILE_TOO_BIG):
        await save_unresolved_review(message, user_id, config, video_info, scan_quality, "deep scan file too big")
        await safe_answer(message, "⚠️ Telegram no permite descargar este archivo para deep scan porque es demasiado grande. Lo dejé pendiente para revisión manual.", config=config)
        await state.clear()
        await after_file_processed(user_id, state, bot, config)
        return True

    search_query = choose_search_query(combined_name, caption_text)
    if config.tmdb_api_key and search_query:
        tmdb_data = await search_tmdb(search_query, config.tmdb_api_key)
        if tmdb_data:
            tmdb_detected = tmdb_result_to_metadata(tmdb_data, scan_quality, combined_name, caption_text)
            if await try_detected(tmdb_detected):
                return True

    if config.llm_api_key and config.llm_model and (search_query or probe_text):
        llm_data = await parse_filename_with_llm(combined_name, caption_text, scan_quality, config)
        if llm_data:
            normalized = normalize_llm_data(llm_data, scan_quality, combined_name, caption_text)
            if await try_detected(normalized):
                return True

    local_data = local_parse_from_ffprobe(probe, scan_quality) or local_parse_from_sources(combined_name, caption_text)
    if local_data:
        if scan_quality and not local_data.get("quality"):
            local_data["quality"] = scan_quality
        if await try_detected(local_data):
            return True

    await save_unresolved_review(message, user_id, config, video_info, scan_quality, "deep scan insufficient metadata")
    await state.clear()
    await after_file_processed(user_id, state, bot, config)
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


def is_allowed(config: Config, user_id: int | None) -> bool:
    return bool(user_id) and (not config.allowed_user_ids or user_id in config.allowed_user_ids)


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
        "/deepscan [status|on|off|timeout <segundos>|maxmb <MB>] - Configurar deep scan\n"
        "/queue - Ver videos pendientes en cola\n"
        "/pending - Ver cuántos archivos requieren revisión\n"
        "/review - Revisar el siguiente archivo pendiente\n"
        "/clearqueue - Vaciar cola pendiente\n"
        "/resetdb confirm - Borrar y recrear la base de datos\n"
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
        f"Deep scan: {'on' if config.deep_scan_enabled else 'off'} (timeout {config.deep_scan_timeout}s, max {config.deep_scan_max_mb} MB)\n"
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


async def deepscan_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    args = command_args(message).split()
    if not args or args[0].lower() == "status":
        await safe_answer(
            message,
            "Deep scan actual:\n"
            f"Estado: {'on' if config.deep_scan_enabled else 'off'}\n"
            f"Timeout: {config.deep_scan_timeout}s\n"
            f"Máximo: {config.deep_scan_max_mb} MB",
        )
        return

    action = args[0].lower()
    if action in {"on", "off"} and len(args) == 1:
        value = action == "on"
        config.deep_scan_enabled = value
        save_setting(config, "deep_scan_enabled", "true" if value else "false")
        await safe_answer(message, f"Deep scan cambiado a {'on' if value else 'off'}.")
        return

    if action == "timeout" and len(args) == 2:
        try:
            value = float(args[1])
        except ValueError:
            value = 0
        if not 0 < value <= 300:
            await safe_answer(message, "Timeout inválido. Usa un valor entre 1 y 300 segundos.")
            return
        config.deep_scan_timeout = value
        save_setting(config, "deep_scan_timeout", value)
        await safe_answer(message, f"Timeout de deep scan cambiado a {value}s.")
        return

    if action == "maxmb" and len(args) == 2:
        try:
            value = int(args[1])
        except ValueError:
            value = 0
        if not 0 < value <= 51200:
            await safe_answer(message, "Máximo inválido. Usa un valor entre 1 y 51200 MB.")
            return
        config.deep_scan_max_mb = value
        save_setting(config, "deep_scan_max_mb", value)
        await safe_answer(message, f"Máximo de deep scan cambiado a {value} MB.")
        return

    await safe_answer(message, "Uso: /deepscan [status|on|off|timeout <segundos>|maxmb <MB>]")


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


async def resetdb_command(message: Message, config: Config) -> None:
    if await reject_if_not_allowed(message, config):
        return
    if command_args(message).lower() != "confirm":
        await safe_answer(message, "Esto borrará la base de datos SQLite. Para confirmar usa: /resetdb confirm")
        return
    reset_database(config)
    await safe_answer(message, "Base de datos borrada y tablas recreadas correctamente.")


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

    search_query = choose_search_query(video_info["file_name"], caption_text)
    tmdb_detected = None

    # 1. Intentar con TMDB primero usando una query saneada.
    if config.tmdb_api_key and search_query:
        tmdb_data = await search_tmdb(search_query, config.tmdb_api_key)
        if tmdb_data:
            tmdb_detected = tmdb_result_to_metadata(tmdb_data, quality, video_info["file_name"], caption_text)
            if tmdb_detected and not required_fields_missing(tmdb_detected):
                if await handle_detected_media(user_id, message, state, bot, config, video_info, tmdb_detected):
                    return

    # 2. Intentar con LLM si TMDB no resolvió suficiente.
    has_useful_context = bool(search_query)
    if config.llm_api_key and config.llm_model and has_useful_context:
        llm_data = await parse_filename_with_llm(video_info["file_name"], caption_text, quality, config)
        
        if llm_data:
            current_data = await state.get_data()
            llm_data = normalize_llm_data(llm_data, quality, video_info["file_name"], caption_text)

            merged_data = {**current_data, **(tmdb_detected or {}), **llm_data}
            if tmdb_detected:
                for key in ("media_type", "name", "year"):
                    if tmdb_detected.get(key):
                        merged_data[key] = tmdb_detected[key]
            merged_data = await enrich_missing_from_tmdb(merged_data, config)
            await state.update_data(**merged_data)

            missing = required_fields_missing(merged_data)
            if missing:
                logging.info("LLM detected partial metadata, missing fields: %s", ", ".join(missing))
            else:
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

    # 3. Si TMDB dio algo parcial y no hay LLM útil, continuar con ese dato antes del fallback local/manual.
    if tmdb_detected:
        tmdb_detected = await enrich_missing_from_tmdb(tmdb_detected, config)
        if not required_fields_missing(tmdb_detected) and await handle_detected_media(user_id, message, state, bot, config, video_info, tmdb_detected):
            return

    # 4. Parsing local solo como fallback, evitando captions genéricos o spam.
    local_data = local_parse_from_sources(video_info["file_name"], caption_text)
    if local_data:
        if quality:
            local_data["quality"] = quality
        local_data = await enrich_missing_from_tmdb(local_data, config)
        if not required_fields_missing(local_data) and await handle_detected_media(user_id, message, state, bot, config, video_info, local_data):
            return

    if await handle_deep_scan_fallback(user_id, message, state, bot, config, video_info, quality):
        return

    if config.llm_auto_post:
        await save_unresolved_review(message, user_id, config, video_info, quality, "insufficient metadata")
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
    await state.update_data(name=clean_detected_name(message.text or ""))
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
    await state.update_data(name=clean_detected_name(message.text or ""))
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

    async def deepscan_handler(message: Message) -> None:
        await deepscan_command(message, config)

    async def queue_handler(message: Message) -> None:
        await queue_command(message, config)

    async def clearqueue_handler(message: Message) -> None:
        await clearqueue_command(message, config)

    async def resetdb_handler(message: Message) -> None:
        await resetdb_command(message, config)

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
    dp.message.register(deepscan_handler, Command("deepscan"), private_chat)
    dp.message.register(queue_handler, Command("queue"), private_chat)
    dp.message.register(pending_handler, Command("pending"), private_chat)
    dp.message.register(review_handler, Command("review"), private_chat)
    dp.message.register(clearqueue_handler, Command("clearqueue"), private_chat)
    dp.message.register(resetdb_handler, Command("resetdb"), private_chat)
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
