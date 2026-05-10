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
```

`BOT_TOKEN`: token de BotFather.

`TARGET_CHANNEL_ID`: ID del canal destino. El bot debe ser administrador del canal con permiso para publicar.

`ALLOWED_USER_IDS`: opcional. IDs de usuarios permitidos separados por coma, por ejemplo `123,456`. Si lo dejas vacío, cualquier usuario puede usarlo.

`TMDB_API_KEY`: opcional. API Key de TMDB. Si la configuras, el bot intentará buscar el título en The Movie Database usando el nombre del archivo y te preguntará si es correcto para ahorrarte escribir el título y el año.

`LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL`: opcionales. Permite conectar el bot a un LLM compatible con la API de OpenAI (Groq, Together, Ollama, etc.). Si está configurado, el bot usará inteligencia artificial para leer el nombre del archivo (y su texto adjunto) y extraer automáticamente si es película o serie, el título, año, temporada, episodio y calidad. Te mostrará el resultado listo para enviar con un solo botón.

`LLM_AUTO_POST`: opcional (`true` o `false`). Si está en `true` y el LLM logra extraer los datos correctamente, el bot publicará el video en el canal inmediatamente sin pedirte confirmación y pasará al siguiente video de la cola. Ideal para envíos masivos.

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
- Para no llenar disco, este bot no descarga archivos. Si quieres renombrar el archivo físicamente antes de publicarlo, habría que descargarlo temporalmente, pero no es recomendable para servidores pequeños.
