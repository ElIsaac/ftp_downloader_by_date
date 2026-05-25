# Descargador SFTP por fecha

Descarga archivos de una carpeta de un servidor SFTP filtrando por fecha de subida. El script guía al usuario paso a paso por una interfaz en consola pensada para gente no técnica.

## Instalación rápida (Windows)

Abre PowerShell (Windows PowerShell 5.1, PowerShell 7+ o pwsh 8) y pega:

```powershell
iex (irm https://raw.githubusercontent.com/ElIsaac/ftp_downloader_by_date/main/install.ps1)
```

El instalador:

1. Detecta Python 3.9+ (te avisa si falta).
2. Descarga el repo (ZIP, no requiere git).
3. Crea un entorno virtual en `%LOCALAPPDATA%\sftp_downloader\.venv`.
4. Instala/valida las dependencias.
5. Copia `.env.example` a `.env` y te ofrece abrirlo en Notepad.
6. Lanza el script.

Para solo instalar sin ejecutar: agrega `-SkipRun` al final.

## Instalación manual

- Python **3.14** o superior (mínimo 3.9 funcionando)
- Dependencias:
  ```
  pip install -r requirements.txt
  ```

## Configuración

1. Copia `.env.example` a `.env`:

   ```
   copy .env.example .env       (Windows)
   cp .env.example .env         (macOS/Linux)
   ```

2. Abre `.env` con un editor de texto y completa:
   - `SFTP_HOST`, `SFTP_PORT`, `SFTP_USER`, `SFTP_PASS`: credenciales del servidor.
   - `LOCAL_DOWNLOAD_ROOT`: carpeta absoluta donde se guardarán las descargas.
   - `MAX_WORKERS` (opcional, 1–16, default 4): cuántos archivos descargar en paralelo.
   - `CONNECT_TIMEOUT` (opcional, default 10): segundos antes de rendirse si el servidor no responde.

## Uso

```
python sftp_downloader.py
```

El script te guía en **4 pasos**:

1. **Conexión** — verifica que puede entrar al servidor.
2. **Carpeta** — te muestra el contenido de la raíz y te pide la ruta a explorar (ej: `facturas/2026`).
3. **Fecha** — te pide la fecha objetivo en formato `DD-MM-AAAA`.
4. **Confirmación** — te muestra un resumen (cuántos archivos, qué tamaño, dónde se guardarán) y te pide confirmar.

Puedes cancelar en cualquier momento con `Ctrl+C`.

## Dónde se guardan las descargas

```
LOCAL_DOWNLOAD_ROOT\
└── <DD-MM-AAAA>\
    └── <nombre_de_la_carpeta>\
        └── ...estructura de subcarpetas del servidor...
```

Solo se descargan los archivos cuya fecha de modificación en el servidor coincida exactamente con la fecha que indicaste. Las subcarpetas locales se crean preservando la jerarquía original.

## Solución de problemas

| Mensaje | Qué hacer |
|---|---|
| `Usuario o contraseña incorrectos` | Revisa `SFTP_USER` y `SFTP_PASS` en `.env` |
| `No se pudo resolver el nombre del servidor` | Verifica `SFTP_HOST` y tu conexión a internet |
| `El servidor no respondió a tiempo` | Aumenta `CONNECT_TIMEOUT` en `.env` |
| `La carpeta "X" no existe en el servidor` | Revisa que el nombre coincida exactamente con el listado mostrado |
| `Formato incorrecto. Necesito DD-MM-AAAA` | Usa guiones y el orden día-mes-año (ej: `15-05-2026`) |

## Notas para Windows

- Si las rutas locales superan 260 caracteres el script te avisa. En Windows 10+ puedes habilitar "long paths" desde el registro o políticas de grupo.
- El script usa siempre `/` para rutas remotas (POSIX) y `\` para rutas locales (Windows) automáticamente — no te preocupes por mezclar los separadores.
