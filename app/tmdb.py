from __future__ import annotations

import logging
import urllib.parse

import aiohttp

from app.config import Config


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
