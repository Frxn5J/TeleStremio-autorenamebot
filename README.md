# Bot de Telegram para renombrar y enviar videos

Bot listo para Coolify. Recibe un video reenviado o subido por el usuario, pregunta si es película o serie, arma el nombre/caption y copia el archivo a un canal sin guardarlo en disco.

## Qué hace

- Acepta videos como `video` o `document` de Telegram.
- Ignora automáticamente archivos multipart como `part1`, `part2`, `cd1`, `cd2`.
- Solo permite extensiones `.mkv` y `.mp4` cuando el archivo trae extensión.
- Detecta calidad desde el nombre del archivo (`1080p`, `720p`, etc.) o desde la altura del video cuando Telegram la provee.
- Envía al canal usando el `file_id` de Telegram, por eso no descarga ni conserva archivos locales.

## Variables de entorno

Configura estas variables en Coolify:

```env
BOT_TOKEN=token_de_botfather
TARGET_CHANNEL_ID=-1001234567890
ALLOWED_USER_IDS=
TMDB_API_KEY=tu_api_key_de_tmdb_aqui
LLM_API_KEY=tu_api_key_del_llm
LLM_BASE_URL=https://api.tu-proveedor.com/v1
LLM_MODEL=nombre-del-modelo
LLM_AUTO_POST=false
LLM_DEBUG=false
LLM_TIMEOUT=15
TELEGRAM_MIN_INTERVAL=1.2
TELEGRAM_FILE_INTERVAL=2.0
TELEGRAM_MAX_RETRIES=8
QUEUE_NOTIFY_EVERY=25
DEEP_SCAN_ENABLED=true
DEEP_SCAN_TIMEOUT=20
DEEP_SCAN_MAX_MB=2048
DATABASE_PATH=/app/data/bot.sqlite3
```

`BOT_TOKEN`: token de BotFather.

`TARGET_CHANNEL_ID`: ID del canal destino. El bot debe ser administrador del canal con permiso para publicar.

`ALLOWED_USER_IDS`: opcional. IDs de usuarios permitidos separados por coma, por ejemplo `123,456`. Si lo dejas vacío, cualquier usuario puede usarlo.

`TMDB_API_KEY`: opcional. API Key de TMDB. Si la configuras, el bot intentará buscar el título en The Movie Database usando el nombre del archivo y te preguntará si es correcto para ahorrarte escribir el título y el año.

`LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL`: opcionales. Permite conectar el bot a un LLM compatible con la API de OpenAI (Groq, Together, Ollama, etc.). Si está configurado, el bot usará inteligencia artificial para leer el nombre del archivo (y su texto adjunto) y extraer automáticamente si es película o serie, el título, año, temporada, episodio y calidad. Te mostrará el resultado listo para enviar con un solo botón.

`LLM_AUTO_POST`: opcional (`true` o `false`). Si está en `true` y el LLM logra extraer los datos correctamente, el bot publicará el video en el canal inmediatamente sin pedirte confirmación y pasará al siguiente video de la cola. Ideal para envíos masivos.

`LLM_DEBUG`: opcional (`true` o `false`). Si está en `true`, el bot mostrará en los logs el prompt enviado al LLM, la respuesta cruda y el JSON parseado. Úsalo solo para depurar porque puede mostrar nombres de archivos o textos privados.

`LLM_TIMEOUT`: segundos máximos para esperar una respuesta del LLM antes de usar detección local/TMDB/manual.

`TELEGRAM_MIN_INTERVAL`: segundos mínimos entre envíos a Telegram. Sube este valor para colas muy grandes.

`TELEGRAM_FILE_INTERVAL`: pausa entre archivo y archivo cuando procesa la cola.

`TELEGRAM_MAX_RETRIES`: reintentos máximos si Telegram responde con timeout o flood control.

`QUEUE_NOTIFY_EVERY`: cada cuántos archivos pendientes avisa el bot en cargas masivas.

`DEEP_SCAN_ENABLED`: opcional (`true` o `false`). Si está activo y la detección normal falla, el bot descarga el archivo a un temporal, intenta leer metadata con `ffprobe` si está instalado, borra siempre el temporal y reintenta TMDB, LLM y parsing local antes de enviarlo a revisión pendiente.

`DEEP_SCAN_TIMEOUT`: segundos máximos para esperar `ffprobe` en la detección secundaria.

`DEEP_SCAN_MAX_MB`: tamaño máximo del archivo para deep scan. Si el archivo supera este límite, no se descarga y pasa al flujo de revisión pendiente.

`DATABASE_PATH`: ruta del archivo SQLite persistente. En Coolify monta un volumen en `/app/data` para conservar la base entre redeploys. La base guarda publicaciones para evitar duplicados y también los cambios hechos con comandos como `/autopost`, `/debug`, `/setchannel` y `/speed`.

## Despliegue rápido en Coolify

1. Sube estos archivos a un repositorio Git.
2. En Coolify crea un nuevo recurso desde ese repositorio.
3. Selecciona despliegue con `Dockerfile`.
4. Agrega las variables de entorno.
5. Despliega.

## Uso

1. Escribe `/start` al bot.
2. Envía o reenvía un archivo de video.
3. Elige `Película` o `Serie`.
4. Completa los campos solicitados.
5. Revisa la vista previa y pulsa `Enviar al canal`.

## Comandos

Estos comandos solo funcionan en chat privado con el bot:

```text
/help - Ver ayuda de comandos
/config - Ver configuración actual
/autopost on|off - Activar/desactivar auto-publicación
/debug on|off - Activar/desactivar logs del LLM
/setchannel -1001234567890 - Cambiar canal destino y guardarlo en SQLite
/speed safe|normal|fast - Cambiar velocidad de procesamiento
/queue - Ver videos pendientes en cola
/pending - Ver archivos que requieren revisión manual
/review - Revisar el siguiente archivo pendiente
/clearqueue - Vaciar cola pendiente
/cancel - Cancelar archivo actual y pasar al siguiente
```

Las API keys se configuran por variables de entorno en Coolify; no se cambian por Telegram para evitar exponer secretos.

## Formatos generados

Película:

```text
Titulo Año Calidad Opcionales.mkv
```

Ejemplo:

```text
Ghosted 2023 1080p WEBRip x265 Dual Audio.mkv
```

Serie:

```text
Titulo.S01E04.Opcionales.1080p.mkv
```

Ejemplo:

```text
Harikatha Sambhavami Yuge Yuge.S01E04.WEB-DL.DDP5.1.1080p.mkv
```

## Notas

- Telegram no permite cambiar realmente el nombre interno del archivo al reenviar/copy usando `file_id`; el bot coloca el nombre final como caption del mismo mensaje en el canal.
- El flujo normal no descarga archivos. Solo si `DEEP_SCAN_ENABLED=true` y la detección normal falla, descarga un temporal para inspección secundaria y lo elimina siempre al terminar.
