from __future__ import annotations

import os
import re
import unicodedata

from aiogram.types import Message


VIDEO_EXTENSIONS = {".mkv", ".mp4"}
MULTIPART_RE = re.compile(r"(?:^|[\s._-])(?:part\s*\d+|cd\s*\d+)(?:[\s._-]|$)", re.IGNORECASE)
SEASON_RE = re.compile(r"^S\d{2,}$", re.IGNORECASE)
EPISODE_RE = re.compile(r"^E\d{2,}$", re.IGNORECASE)
QUALITY_RE = re.compile(r"(2160p|1440p|1080p|720p|576p|540p|480p|360p)", re.IGNORECASE)
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
SOCIAL_TOKEN_RE = re.compile(r"(?:^|\s)[@#][\w.-]+", re.IGNORECASE)
LONG_ID_RE = re.compile(r"\b\d{7,}\b")
LOOSE_TIME_RE = re.compile(r"\b\d{1,2}(?:[:.]\d{2}){1,2}\b")
SPAM_TOKEN_RE = re.compile(
    r"\b(?:ver|mira|mirar|online|latino|hd|pelisplus|pelis|plus|cuevana|repelis|gnula|descargar|download|gratis|free|full|"
    r"completa|completo|castellano|subtitulado|sub|espanol|español|mega|telegram|canal|official|oficial)\b",
    re.IGNORECASE,
)
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
VARIATION_SELECTOR_RE = re.compile(r"[\ufe00-\ufe0f\u20e3\U000e0100-\U000e01ef]")
ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u2060\ufeff]")


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
    name = strip_noise_text(name)
    # Remove quality, codecs, years, episodes etc to get just the title
    name = re.sub(r"\b(19|20)\d{2}\b.*", "", name) # Remove year and everything after
    name = re.sub(r"S\d{2}E\d{2}.*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"(2160p|1440p|1080p|720p|576p|540p|480p|360p).*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[\._-]", " ", name)
    return clean_text(name).strip(" .-_")


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
    value = strip_noise_text(value)
    value = re.sub(r"[._-]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def strip_noise_text(value: str) -> str:
    value = URL_RE.sub(" ", value or "")
    value = SOCIAL_TOKEN_RE.sub(" ", value)
    value = LONG_ID_RE.sub(" ", value)
    value = LOOSE_TIME_RE.sub(" ", value)
    value = SPAM_TOKEN_RE.sub(" ", value)
    value = re.sub(r"\b(?:0?[1-9]|[12]\d|3[01])\s+(?:0?[1-9]|1[0-2])\s+\d{2,4}\b", " ", value)
    return clean_text(value)


def strip_decorative_symbols(value: str) -> str:
    value = VARIATION_SELECTOR_RE.sub("", value or "")
    value = ZERO_WIDTH_RE.sub("", value)
    chars = []
    for char in value:
        category = unicodedata.category(char)
        if category in {"So", "Sk"}:
            chars.append(" ")
            continue
        if category == "Cf":
            continue
        chars.append(char)
    return "".join(chars)


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
    value = clean_text(value).strip(" .")
    return "" if is_generic_title(value) else value


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


def tmdb_result_to_metadata(tmdb_data: dict, quality: str | None, file_name: str, caption: str) -> dict | None:
    title = clean_detected_name(tmdb_data.get("title") or tmdb_data.get("name") or "")
    if not title:
        return None

    media_type = tmdb_data.get("media_type")
    date = tmdb_data.get("release_date") or tmdb_data.get("first_air_date") or ""
    result: dict = {
        "media_type": "series" if media_type == "tv" else "movie",
        "name": title,
        "quality": quality or "",
        "optional": "",
    }
    if result["media_type"] == "movie" and date:
        result["year"] = date[:4]

    local_data = local_parse_from_sources(file_name, caption)
    if local_data and local_data.get("media_type") == result["media_type"]:
        for key in ("season", "episode", "year"):
            if local_data.get(key) and not result.get(key):
                result[key] = local_data[key]
        result["optional"] = clean_optional(local_data.get("optional", ""))

    return result


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

    if result.get("name"):
        result["name"] = clean_detected_name(str(result["name"]))
    result["optional"] = clean_optional(str(result.get("optional") or ""))

    return result


def required_fields_missing(data: dict) -> list[str]:
    if data.get("media_type") == "movie":
        return [key for key in ("name", "year", "quality") if not data.get(key) or (key == "name" and not clean_detected_name(str(data.get(key))))]
    if data.get("media_type") == "series":
        return [key for key in ("name", "season", "episode", "quality") if not data.get(key) or (key == "name" and not clean_detected_name(str(data.get(key))))]
    return ["media_type"]


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
    value = strip_decorative_symbols(value)
    return re.sub(r"\s+", " ", value.strip())


def clean_optional(value: str) -> str:
    value = clean_text(strip_noise_text(value))
    if value in {"-", ".", "no", "No", "NO", "ninguno", "Ninguno"}:
        return ""
    value = QUALITY_RE.sub("", value)
    value = re.sub(r"\b(?:mp4|mkv|avi)\b|\.(?:mp4|mkv|avi)$", "", value, flags=re.IGNORECASE)
    value = clean_text(value).strip(" .")
    return "" if is_generic_title(value) else value


def normalize_quality(value: str) -> str:
    match = QUALITY_RE.search(value)
    return match.group(1).lower() if match else clean_text(value)


def ensure_extension(file_name: str) -> str:
    ext = get_extension(file_name)
    return ext if ext in VIDEO_EXTENSIONS else ".mkv"


def build_caption(data: dict) -> str:
    ext = ensure_extension(data["file_name"])
    optional = clean_optional(str(data.get("optional", "")))
    name = clean_detected_name(str(data.get("name", "")))

    if data["media_type"] == "movie":
        parts = [name, data["year"], data["quality"]]
        if optional:
            parts.append(optional)
        return f"{' '.join(parts)}{ext}"

    parts = [f"{name}.{data['season']}{data['episode']}"]
    if optional:
        parts.append(optional.replace(" ", "."))
    parts.append(data["quality"])
    return f"{'.'.join(parts)}{ext}"


def current_file_label(data: dict) -> str:
    return ""
