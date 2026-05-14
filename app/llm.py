from __future__ import annotations

import json
import logging
import re

from openai import AsyncOpenAI

from app.config import Config, env_float


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
