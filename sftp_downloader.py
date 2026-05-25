"""Descargador SFTP por fecha.

Script de un solo archivo con tres capas:
- Modelos (dataclasses frozen)
- Servicios (responsabilidad única)
- Orquestador (main)

Pensado para usuarios no técnicos: flujo guiado en 4 pasos, mensajes en
español, confirmación antes de cada acción que toma tiempo.
"""

import locale
import posixpath
import queue
import socket
import stat as stat_mod
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Iterator

import paramiko
from dotenv import dotenv_values
from paramiko import SFTPAttributes, SFTPClient, Transport
from paramiko.ssh_exception import AuthenticationException, SSHException
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.prompt import Prompt
from rich.table import Table


# ─────────────────────────────────────────────────────────────────────────────
# Modelos
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SFTPCredentials:
    host: str
    port: int
    username: str
    password: str


@dataclass(frozen=True, slots=True)
class AppConfig:
    credentials: SFTPCredentials
    local_download_root: Path
    max_workers: int
    connect_timeout: float


@dataclass(frozen=True, slots=True)
class ConnectionDiagnostic:
    ok: bool
    latency_ms: float
    server_banner: str | None
    error_message: str | None


@dataclass(frozen=True, slots=True)
class RemoteEntry:
    name: str
    full_path: str
    is_dir: bool
    size_bytes: int
    modified_at: datetime


@dataclass(frozen=True, slots=True)
class DateFilter:
    start_date: date
    end_date: date

    @property
    def is_single_day(self) -> bool:
        return self.start_date == self.end_date

    def describe(self) -> str:
        s = self.start_date.strftime("%d-%m-%Y")
        if self.is_single_day:
            return s
        return f"{s} al {self.end_date.strftime('%d-%m-%Y')}"

    def folder_name(self) -> str:
        s = self.start_date.strftime("%d-%m-%Y")
        if self.is_single_day:
            return s
        return f"{s}_al_{self.end_date.strftime('%d-%m-%Y')}"


@dataclass(frozen=True, slots=True)
class DownloadPlan:
    remote_root: str
    entries: list[RemoteEntry]
    local_destination: Path
    total_bytes: int


@dataclass(frozen=True, slots=True)
class DownloadResult:
    entry: RemoteEntry
    success: bool
    local_path: Path | None
    error: str | None
    duration_sec: float


@dataclass(frozen=True, slots=True)
class DownloadSummary:
    total: int
    successes: int
    failures: int
    total_bytes: int
    duration_sec: float
    results: list[DownloadResult]


class ConfigError(Exception):
    """Error de configuración en .env, mostrado al usuario tal cual."""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de formato
# ─────────────────────────────────────────────────────────────────────────────


def format_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f} segundos"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes} min {secs} s"


def format_date_human(d: date) -> str:
    for loc in ("es_ES.UTF-8", "es_ES.utf8", "Spanish_Spain.1252", "es_MX.UTF-8"):
        try:
            locale.setlocale(locale.LC_TIME, loc)
            return d.strftime("%A %d de %B de %Y")
        except locale.Error:
            continue
    return d.strftime("%d-%m-%Y")


# ─────────────────────────────────────────────────────────────────────────────
# ErrorTranslator
# ─────────────────────────────────────────────────────────────────────────────


class ErrorTranslator:
    @staticmethod
    def translate(exc: BaseException) -> str:
        match exc:
            case AuthenticationException():
                return (
                    "Usuario o contraseña incorrectos. "
                    "Revisa SFTP_USER y SFTP_PASS en tu archivo .env."
                )
            case socket.gaierror():
                return (
                    "No se pudo resolver el nombre del servidor. "
                    "Revisa SFTP_HOST y tu conexión a internet."
                )
            case socket.timeout():
                return (
                    "El servidor no respondió a tiempo. "
                    "Revisa tu conexión o aumenta CONNECT_TIMEOUT en .env."
                )
            case PermissionError():
                return "Permiso denegado."
            case FileNotFoundError():
                return "La ruta indicada no existe en el servidor."
            case SSHException():
                return f"Error de conexión SSH: {exc}"
            case _:
                return f"{type(exc).__name__}: {exc}"


# ─────────────────────────────────────────────────────────────────────────────
# Servicios
# ─────────────────────────────────────────────────────────────────────────────


class EnvConfigLoader:
    REQUIRED = ("SFTP_HOST", "SFTP_PORT", "SFTP_USER", "SFTP_PASS", "LOCAL_DOWNLOAD_ROOT")

    def load(self, env_path: Path = Path(".env")) -> AppConfig:
        if not env_path.exists():
            raise ConfigError(
                f"No encontré el archivo {env_path.resolve()}.\n"
                "Crea uno usando .env.example como plantilla."
            )

        values = dotenv_values(env_path)
        missing = [k for k in self.REQUIRED if not values.get(k)]
        if missing:
            raise ConfigError(
                "Faltan estas variables en .env: " + ", ".join(missing)
            )

        try:
            port = int(values["SFTP_PORT"])
        except ValueError:
            raise ConfigError("SFTP_PORT debe ser un número entero (ej: 22)")

        credentials = SFTPCredentials(
            host=values["SFTP_HOST"].strip(),
            port=port,
            username=values["SFTP_USER"].strip(),
            password=values["SFTP_PASS"],
        )

        root_raw = values["LOCAL_DOWNLOAD_ROOT"].strip()
        local_root = Path(root_raw)
        if not local_root.is_absolute():
            raise ConfigError(
                f"LOCAL_DOWNLOAD_ROOT debe ser una ruta absoluta.\n"
                f"Recibí: {root_raw}"
            )
        local_root.mkdir(parents=True, exist_ok=True)

        try:
            max_workers = int(values.get("MAX_WORKERS", "4"))
        except ValueError:
            raise ConfigError("MAX_WORKERS debe ser un número entero entre 1 y 16")
        if not 1 <= max_workers <= 16:
            raise ConfigError(
                f"MAX_WORKERS debe estar entre 1 y 16. Recibí: {max_workers}"
            )

        try:
            connect_timeout = float(values.get("CONNECT_TIMEOUT", "10"))
        except ValueError:
            raise ConfigError("CONNECT_TIMEOUT debe ser un número (segundos)")

        return AppConfig(
            credentials=credentials,
            local_download_root=local_root,
            max_workers=max_workers,
            connect_timeout=connect_timeout,
        )


class SFTPConnectionService:
    def __init__(self, config: AppConfig):
        self._config = config
        self._transport: Transport | None = None
        self._main_client: SFTPClient | None = None

    def __enter__(self) -> "SFTPConnectionService":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def connect(self) -> SFTPClient:
        creds = self._config.credentials
        sock = socket.create_connection(
            (creds.host, creds.port), timeout=self._config.connect_timeout
        )
        self._transport = Transport(sock)
        self._transport.banner_timeout = self._config.connect_timeout
        self._transport.connect(username=creds.username, password=creds.password)
        self._main_client = SFTPClient.from_transport(self._transport)
        if self._main_client is None:
            raise SSHException("No se pudo abrir el canal SFTP")
        return self._main_client

    @property
    def client(self) -> SFTPClient:
        if self._main_client is None:
            raise RuntimeError("Conexión no iniciada")
        return self._main_client

    @property
    def server_banner(self) -> str | None:
        if self._transport is None:
            return None
        banner = self._transport.get_banner()
        if not banner:
            return None
        return banner.decode("utf-8", errors="replace").strip()

    def open_thread_client(self) -> SFTPClient:
        if self._transport is None:
            raise RuntimeError("Conexión no iniciada")
        client = SFTPClient.from_transport(self._transport)
        if client is None:
            raise SSHException("No se pudo abrir un canal SFTP adicional")
        return client

    def close(self) -> None:
        if self._main_client is not None:
            try:
                self._main_client.close()
            except Exception:
                pass
            self._main_client = None
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                pass
            self._transport = None


class ConnectionDiagnosticService:
    def run(self, client: SFTPClient, banner: str | None) -> ConnectionDiagnostic:
        start = time.perf_counter()
        try:
            client.listdir(".")
        except Exception as e:
            return ConnectionDiagnostic(
                ok=False,
                latency_ms=0.0,
                server_banner=banner,
                error_message=ErrorTranslator.translate(e),
            )
        latency_ms = (time.perf_counter() - start) * 1000
        return ConnectionDiagnostic(
            ok=True, latency_ms=latency_ms, server_banner=banner, error_message=None
        )


class RemoteListingService:
    def __init__(self, client: SFTPClient):
        self._client = client

    def list_dir(self, path: str) -> list[RemoteEntry]:
        return [self._to_entry(a, path) for a in self._client.listdir_attr(path)]

    def validate_path(self, path: str) -> bool:
        try:
            attrs = self._client.stat(path)
        except (IOError, OSError):
            return False
        return self._is_dir(attrs.st_mode)

    def _to_entry(self, attr: SFTPAttributes, parent: str) -> RemoteEntry:
        full_path = posixpath.join(parent, attr.filename)
        if full_path.startswith("./"):
            full_path = full_path[2:]
        return RemoteEntry(
            name=attr.filename,
            full_path=full_path,
            is_dir=self._is_dir(attr.st_mode),
            size_bytes=attr.st_size or 0,
            modified_at=datetime.fromtimestamp(attr.st_mtime or 0),
        )

    @staticmethod
    def _is_dir(mode: int | None) -> bool:
        return mode is not None and stat_mod.S_ISDIR(mode)


class ParallelRemoteWalker:
    """Recorre un árbol SFTP usando varios canales en paralelo.

    Cada worker abre su propio canal SFTP sobre el mismo Transport y procesa
    directorios en paralelo. Mientras el walk típico (un canal) hace N
    round-trips serializados para N carpetas, este hace ~N/W con W workers.
    """

    def __init__(self, connection: SFTPConnectionService, max_workers: int):
        self._connection = connection
        self._max_workers = max(1, max_workers)
        self.pruned_count = 0

    def walk(
        self,
        root: str,
        errors: list[str] | None = None,
        prune: Callable[[RemoteEntry], bool] | None = None,
    ) -> Iterator[RemoteEntry]:
        # Abrir todos los canales al inicio. Si el servidor limita sesiones,
        # caemos a la cantidad que sí pudo abrir (al menos 1).
        clients: list[SFTPClient] = []
        for _ in range(self._max_workers):
            try:
                clients.append(self._connection.open_thread_client())
            except Exception:
                if not clients:
                    raise
                break
        n_workers = len(clients)

        dirs_q: queue.Queue = queue.Queue()
        results_q: queue.Queue = queue.Queue()
        dirs_q.put(root)
        pending = [1]
        lock = threading.Lock()
        self.pruned_count = 0

        def to_entry(attr: SFTPAttributes, parent: str) -> RemoteEntry:
            full_path = posixpath.join(parent, attr.filename)
            if full_path.startswith("./"):
                full_path = full_path[2:]
            is_dir = attr.st_mode is not None and stat_mod.S_ISDIR(attr.st_mode)
            return RemoteEntry(
                name=attr.filename,
                full_path=full_path,
                is_dir=is_dir,
                size_bytes=attr.st_size or 0,
                modified_at=datetime.fromtimestamp(attr.st_mtime or 0),
            )

        def worker(client: SFTPClient) -> None:
            try:
                while True:
                    current = dirs_q.get()
                    if current is None:
                        return
                    new_dirs: list[str] = []
                    local_pruned = 0
                    try:
                        attrs_list = client.listdir_attr(current)
                    except OSError:
                        results_q.put(("error", current))
                    else:
                        batch: list[RemoteEntry] = []
                        for attr in attrs_list:
                            entry = to_entry(attr, current)
                            if entry.is_dir and prune is not None and prune(entry):
                                local_pruned += 1
                                continue
                            batch.append(entry)
                            if entry.is_dir:
                                new_dirs.append(entry.full_path)
                        if batch:
                            results_q.put(("entries", batch))

                    with lock:
                        if local_pruned:
                            self.pruned_count += local_pruned
                        pending[0] += len(new_dirs) - 1
                        all_done = pending[0] == 0
                    for d in new_dirs:
                        dirs_q.put(d)
                    if all_done:
                        for _ in range(n_workers):
                            dirs_q.put(None)
                        results_q.put(("done", None))
            finally:
                try:
                    client.close()
                except Exception:
                    pass

        threads = [
            threading.Thread(target=worker, args=(c,), daemon=True)
            for c in clients
        ]
        for t in threads:
            t.start()

        try:
            while True:
                kind, payload = results_q.get()
                if kind == "entries":
                    yield from payload
                elif kind == "error":
                    if errors is not None:
                        errors.append(payload)
                elif kind == "done":
                    # Drenar lo que quede en la cola
                    while True:
                        try:
                            kind2, payload2 = results_q.get_nowait()
                        except queue.Empty:
                            break
                        if kind2 == "entries":
                            yield from payload2
                        elif kind2 == "error" and errors is not None:
                            errors.append(payload2)
                    break
        finally:
            for t in threads:
                t.join(timeout=2.0)


class DateParser:
    @staticmethod
    def parse(raw: str) -> date:
        try:
            return datetime.strptime(raw.strip(), "%d-%m-%Y").date()
        except ValueError:
            raise ValueError(f"'{raw}' no es una fecha válida en formato DD-MM-AAAA")


class DateFilterService:
    def apply(
        self, entries: Iterable[RemoteEntry], date_filter: DateFilter
    ) -> list[RemoteEntry]:
        return [
            e
            for e in entries
            if not e.is_dir
            and date_filter.start_date
            <= e.modified_at.date()
            <= date_filter.end_date
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Presentación
# ─────────────────────────────────────────────────────────────────────────────


class ConsoleRenderer:
    TOTAL_STEPS = 4

    def __init__(self) -> None:
        self.console = Console()

    def welcome(self) -> None:
        panel = Panel(
            "[bold]Descargador SFTP por fecha[/]\n\n"
            "Descarga archivos de una carpeta del servidor\n"
            "filtrando por fecha de subida.",
            border_style="cyan",
            padding=(1, 2),
        )
        self.console.print()
        self.console.print(panel)
        self.console.print()
        self.console.print(
            f"Esto tomará [bold]{self.TOTAL_STEPS} pasos[/]. "
            "Puedes cancelar en cualquier momento con [bold]Ctrl+C[/]."
        )
        self.console.print()
        try:
            input("Presiona Enter para empezar...")
        except EOFError:
            pass

    def step(self, n: int, title: str) -> None:
        self.console.print()
        self.console.print(f"[bold cyan]Paso {n} de {self.TOTAL_STEPS} · {title}[/]")
        self.console.print("[cyan]" + "─" * 50 + "[/]")
        self.console.print()

    def success(self, msg: str) -> None:
        self.console.print(f"[green]✓[/] {msg}")

    def error(self, msg: str, hint: str | None = None) -> None:
        self.console.print(f"[red]✗[/] {msg}")
        if hint:
            self.console.print(f"  [dim]{hint}[/]")

    def warning(self, msg: str) -> None:
        self.console.print(f"[yellow]⚠[/] {msg}")

    def info(self, msg: str) -> None:
        self.console.print(f"  {msg}")

    def navigation_view(
        self,
        current_path: str,
        entries: list[RemoteEntry],
        can_go_up: bool,
    ) -> str:
        self.console.print()
        display = "/" if current_path == "." else current_path
        self.console.print(f"Estás en: [bold cyan]{display}[/]")
        self.console.print()

        if not entries:
            self.console.print("  [dim](carpeta vacía)[/]")
        else:
            for e in entries:
                date_str = e.modified_at.strftime("%d-%m-%Y")
                if e.is_dir:
                    self.console.print(
                        f"  📁  {e.name}  [dim]· {date_str}[/]"
                    )
                else:
                    self.console.print(
                        f"  📄  {e.name}"
                        f"  [dim]· {date_str} · "
                        f"{format_bytes(e.size_bytes)}[/]"
                    )

        self.console.print()
        self.console.print(
            "[dim]Acciones:[/]\n"
            "  [dim]· <nombre carpeta> + Enter      →  entrar a esa carpeta[/]\n"
            "  [dim]· <ruta/sub/sub> + Enter        →  "
            "navegar directo (relativa o absoluta con /)[/]\n"
            + (
                "  [dim]· .. + Enter                    →  "
                "subir un nivel[/]\n"
                if can_go_up
                else ""
            )
            + "  [dim]· d + Enter                     →  "
            "descargar esta carpeta (con confirmación)[/]"
        )
        raw = Prompt.ask(
            "[bold cyan]>[/]",
            console=self.console,
            default="",
            show_default=False,
        )
        return raw.strip()

    def preview_panel(self, plan: DownloadPlan, date_filter: DateFilter) -> None:
        if date_filter.is_single_day:
            fecha_human = format_date_human(date_filter.start_date)
            fecha_line = (
                f"  Fecha:           [bold]"
                f"{date_filter.start_date.strftime('%d-%m-%Y')}[/]\n"
                f"                   [dim]({fecha_human})[/]\n\n"
            )
        else:
            fecha_line = (
                f"  Rango fechas:    [bold]"
                f"{date_filter.start_date.strftime('%d-%m-%Y')}[/]  al  "
                f"[bold]{date_filter.end_date.strftime('%d-%m-%Y')}[/]\n\n"
            )
        body = (
            f"  Carpeta origen:  [bold]{plan.remote_root}[/]\n"
            f"{fecha_line}"
            f"  Archivos:        [bold]{len(plan.entries)}[/]\n"
            f"  Tamaño total:    [bold]{format_bytes(plan.total_bytes)}[/]\n\n"
            f"  Destino local:\n"
            f"    [dim]{plan.local_destination}[/]"
        )
        self.console.print(
            Panel(body, title="Resumen", border_style="cyan", padding=(1, 2))
        )
        self.console.print("\nPrimeros archivos a descargar:")
        preview_count = min(5, len(plan.entries))
        for entry in plan.entries[:preview_count]:
            rel = posixpath.relpath(entry.full_path, plan.remote_root)
            self.console.print(
                f"  • {rel}  [dim]({format_bytes(entry.size_bytes)})[/]"
            )
        remaining = len(plan.entries) - preview_count
        if remaining > 0:
            self.console.print(f"  [dim]… y {remaining} más[/]")
        self.console.print()

    def progress_context(self) -> Progress:
        return Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[bold]{task.percentage:>3.0f}%[/]"),
            TextColumn("·"),
            TransferSpeedColumn(),
            TextColumn("·"),
            TimeRemainingColumn(),
            console=self.console,
        )

    def summary_panel(
        self,
        summary: DownloadSummary,
        destination: Path,
        walk_errors: list[str] | None = None,
    ) -> None:
        ok_color = "green" if summary.failures == 0 else "yellow"
        speed_mbs = (
            (summary.total_bytes / max(summary.duration_sec, 0.001)) / 1024 / 1024
        )
        body = (
            f"  [{ok_color}]✓ {summary.successes} archivos descargados "
            f"({format_bytes(summary.total_bytes)})[/]\n"
            f"  [red]✗ {summary.failures} fallos[/]\n\n"
            f"  Tiempo:     [bold]{format_duration(summary.duration_sec)}[/]\n"
            f"  Velocidad:  [bold]{speed_mbs:.1f} MB/s[/]\n\n"
            f"  Guardado en:\n"
            f"    [dim]{destination}[/]"
        )
        if summary.failures == 0:
            title = "Descarga completada"
            border = "green"
        else:
            title = "Descarga finalizada con fallos"
            border = "yellow"
        self.console.print()
        self.console.print(
            Panel(body, title=title, border_style=border, padding=(1, 2))
        )

        if summary.failures > 0:
            self.console.print("\n[bold red]Archivos con fallo:[/]")
            for r in summary.results:
                if not r.success:
                    self.console.print(
                        f"  • {r.entry.full_path}  [red]{r.error}[/]"
                    )

        if walk_errors:
            self.console.print(
                "\n[bold yellow]Carpetas inaccesibles "
                "(no se exploraron, sus archivos no se descargaron):[/]"
            )
            for p in walk_errors:
                self.console.print(f"  • {p}")

        self.console.print()
        try:
            input("Presiona Enter para salir...")
        except EOFError:
            pass

    def goodbye(self) -> None:
        self.console.print("\n[dim]Hasta pronto.[/]\n")

    def prompt(self, message: str) -> str:
        return Prompt.ask(f"[bold cyan]{message}[/]", console=self.console)

    def confirm(self, message: str, default: bool = True) -> bool:
        # Rich Confirm.ask sólo acepta y/n y muestra "Please enter Y or N" en
        # cualquier otro caso. Hacemos uno propio que muestra [y/n] pero
        # también acepta s/sí (y mayúsculas) silenciosamente.
        default_hint = "y" if default else "n"
        while True:
            raw = Prompt.ask(
                f"[bold cyan]{message}[/] [dim]\\[y/n] ({default_hint})[/]",
                console=self.console,
                default="",
                show_default=False,
            )
            v = raw.strip().lower()
            if v == "":
                return default
            if v in ("y", "yes", "s", "si", "sí"):
                return True
            if v in ("n", "no"):
                return False
            self.console.print("[yellow]  Responde con y o n.[/]")


class UserPromptService:
    def __init__(
        self, renderer: ConsoleRenderer, listing_service: RemoteListingService
    ):
        self._renderer = renderer
        self._listing = listing_service

    def navigate_to_folder(self) -> str:
        """Navegación interactiva. Sólo lista la carpeta actual (no recursivo).

        Retorna el path remoto de la carpeta que el usuario eligió descargar,
        o "." si eligió la raíz.
        """
        current = "."

        while True:
            try:
                entries = self._listing.list_dir(current)
            except PermissionError:
                self._renderer.error(
                    f"No tienes permiso para ver el contenido de "
                    f"'{current}' en el servidor.",
                    "Vuelvo a la carpeta anterior.",
                )
                if current == ".":
                    raise
                current = posixpath.dirname(current.rstrip("/")) or "."
                continue
            except (IOError, OSError) as e:
                self._renderer.error(
                    f"No se pudo leer '{current}' del servidor.",
                    ErrorTranslator.translate(e),
                )
                if current == ".":
                    raise
                current = posixpath.dirname(current.rstrip("/")) or "."
                continue

            sorted_entries = sorted(
                entries, key=lambda x: (not x.is_dir, x.name.lower())
            )

            choice = self._renderer.navigation_view(
                current, sorted_entries, can_go_up=(current != ".")
            )

            if choice == "" or choice.lower() in ("d", "descargar"):
                has_files = any(not e.is_dir for e in sorted_entries)
                has_subdirs = any(e.is_dir for e in sorted_entries)
                if not has_files and not has_subdirs:
                    self._renderer.error(
                        "Esta carpeta está vacía, no hay nada que descargar."
                    )
                    continue
                display = "/" if current == "." else current
                self._renderer.console.print()
                self._renderer.console.print(
                    f"Has elegido la carpeta: [bold cyan]{display}[/]"
                )
                self._renderer.console.print(
                    "A continuación te preguntaré el [bold]rango de fechas[/] "
                    "de los archivos a descargar."
                )
                if current == ".":
                    self._renderer.warning(
                        "Estás eligiendo la RAÍZ del servidor "
                        "(la descarga podría ser muy grande)."
                    )
                if not self._renderer.confirm(
                    "¿Es ésta la carpeta correcta?",
                    default=True,
                ):
                    self._renderer.console.print(
                        "[dim]Sigues navegando.[/]"
                    )
                    continue
                return current

            if choice == "..":
                if current == ".":
                    self._renderer.error("Ya estás en la raíz.")
                    continue
                current = posixpath.dirname(current.rstrip("/")) or "."
                continue

            if "/" in choice:
                if choice.startswith("/"):
                    target = posixpath.normpath(choice.lstrip("/"))
                elif current == ".":
                    target = posixpath.normpath(choice)
                else:
                    target = posixpath.normpath(
                        posixpath.join(current, choice)
                    )
                if target in ("", "."):
                    current = "."
                    continue
                if target.startswith(".."):
                    self._renderer.error(
                        f"La ruta '{choice}' sale del servidor.",
                        "Usa una ruta dentro del servidor.",
                    )
                    continue
                if not self._listing.validate_path(target):
                    self._renderer.error(
                        f"La ruta '{choice}' no existe o no es una carpeta.",
                        "Recuerda que distingue mayúsculas y minúsculas.",
                    )
                    continue
                current = target
                continue

            matched = next(
                (e for e in sorted_entries if e.name == choice), None
            )
            if matched is None:
                self._renderer.error(
                    f"'{choice}' no existe en esta carpeta.",
                    "Escribe el nombre EXACTO, una ruta como "
                    "'carpeta/subcarpeta', '..' para subir, "
                    "o 'd' para descargar esta carpeta.",
                )
                continue

            if not matched.is_dir:
                self._renderer.error(
                    "Sólo puedes entrar a carpetas, no a archivos.",
                    "Para descargar, escribe 'd' cuando estés en la carpeta deseada.",
                )
                continue

            current = matched.full_path

    def ask_date_range(self) -> DateFilter:
        c = self._renderer.console
        c.print(
            "Necesito un [bold]rango de fechas[/] (inclusivo en ambos extremos)."
        )
        c.print("Formato: [bold]DD-MM-AAAA[/]  (día-mes-año, con guiones)")
        c.print("[dim]Ejemplo: 15-05-2026[/]")
        c.print(
            "[dim]Tip: si quieres un solo día, escribe la misma fecha en "
            "inicio y fin.[/]"
        )
        c.print()

        while True:
            raw = self._renderer.prompt("Fecha de inicio")
            try:
                start = DateParser.parse(raw)
                break
            except ValueError:
                self._renderer.error(
                    "Formato incorrecto. Necesito DD-MM-AAAA.",
                    "Ejemplo válido: 15-05-2026. Inténtalo otra vez.",
                )

        while True:
            raw = self._renderer.prompt("Fecha de fin")
            try:
                end = DateParser.parse(raw)
            except ValueError:
                self._renderer.error(
                    "Formato incorrecto. Necesito DD-MM-AAAA.",
                    "Ejemplo válido: 15-05-2026. Inténtalo otra vez.",
                )
                continue
            if end < start:
                self._renderer.error(
                    "La fecha de fin es anterior a la de inicio.",
                    f"Inicio: {start.strftime('%d-%m-%Y')}. "
                    "Escribe una fecha igual o posterior.",
                )
                continue
            return DateFilter(start_date=start, end_date=end)

    def confirm(self, message: str, default: bool = True) -> bool:
        return self._renderer.confirm(message, default=default)


# ─────────────────────────────────────────────────────────────────────────────
# Descarga
# ─────────────────────────────────────────────────────────────────────────────


class DownloadService:
    def __init__(
        self,
        connection: SFTPConnectionService,
        config: AppConfig,
        renderer: ConsoleRenderer,
    ):
        self._connection = connection
        self._config = config
        self._renderer = renderer
        self._thread_local = threading.local()
        self._lock = threading.Lock()
        self._opened_clients: list[SFTPClient] = []

    def run(self, plan: DownloadPlan) -> DownloadSummary:
        # 1. Crear todos los directorios locales antes (evita race conditions).
        for entry in plan.entries:
            local_path = self._compute_local_path(entry, plan)
            local_path.parent.mkdir(parents=True, exist_ok=True)

        results: list[DownloadResult] = []
        start = time.perf_counter()

        try:
            with self._renderer.progress_context() as progress:
                task_id = progress.add_task(
                    "Descargando...", total=max(plan.total_bytes, 1)
                )
                with ThreadPoolExecutor(
                    max_workers=self._config.max_workers
                ) as executor:
                    futures = {
                        executor.submit(
                            self._download_one, entry, plan, progress, task_id
                        ): entry
                        for entry in plan.entries
                    }
                    for future in as_completed(futures):
                        results.append(future.result())
        finally:
            self._close_thread_clients()

        duration = time.perf_counter() - start
        successes = sum(1 for r in results if r.success)
        failures = len(results) - successes
        total_bytes = sum(r.entry.size_bytes for r in results if r.success)

        return DownloadSummary(
            total=len(results),
            successes=successes,
            failures=failures,
            total_bytes=total_bytes,
            duration_sec=duration,
            results=results,
        )

    def _download_one(
        self,
        entry: RemoteEntry,
        plan: DownloadPlan,
        progress: Progress,
        task_id: int,
    ) -> DownloadResult:
        client = self._get_thread_client()
        local_path = self._compute_local_path(entry, plan)
        start = time.perf_counter()
        try:
            client.get(entry.full_path, str(local_path))
            duration = time.perf_counter() - start
            with self._lock:
                progress.update(task_id, advance=entry.size_bytes)
            return DownloadResult(
                entry=entry,
                success=True,
                local_path=local_path,
                error=None,
                duration_sec=duration,
            )
        except Exception as e:
            duration = time.perf_counter() - start
            with self._lock:
                progress.update(task_id, advance=entry.size_bytes)
            return DownloadResult(
                entry=entry,
                success=False,
                local_path=None,
                error=ErrorTranslator.translate(e),
                duration_sec=duration,
            )

    def _get_thread_client(self) -> SFTPClient:
        client = getattr(self._thread_local, "client", None)
        if client is None:
            client = self._connection.open_thread_client()
            self._thread_local.client = client
            with self._lock:
                self._opened_clients.append(client)
        return client

    def _close_thread_clients(self) -> None:
        for c in self._opened_clients:
            try:
                c.close()
            except Exception:
                pass
        self._opened_clients.clear()

    @staticmethod
    def _compute_local_path(entry: RemoteEntry, plan: DownloadPlan) -> Path:
        relative = posixpath.relpath(entry.full_path, plan.remote_root)
        parts = relative.split("/")
        return plan.local_destination.joinpath(*parts)


# ─────────────────────────────────────────────────────────────────────────────
# Orquestador
# ─────────────────────────────────────────────────────────────────────────────


def _yyyymmdd_prune_predicate(
    date_filter: DateFilter, margin_days: int = 1
) -> Callable[[RemoteEntry], bool]:
    """Predicado que poda carpetas cuyo nombre `YYYYMMDD` cae fuera del rango.

    El margen de ±N días absorbe el desfase típico entre el nombre de la
    carpeta y el mtime de los archivos dentro (observado: +1 día).
    """
    lo = date_filter.start_date - timedelta(days=margin_days)
    hi = date_filter.end_date + timedelta(days=margin_days)

    def prune(entry: RemoteEntry) -> bool:
        name = entry.name
        if len(name) != 8 or not name.isdigit():
            return False
        try:
            folder_date = datetime.strptime(name, "%Y%m%d").date()
        except ValueError:
            return False
        return folder_date < lo or folder_date > hi

    return prune


def _walk_filter_with_progress(
    walker: "ParallelRemoteWalker",
    remote_root: str,
    date_filter: DateFilter,
    renderer: "ConsoleRenderer",
    errors: list[str],
) -> list[RemoteEntry]:
    """Recorre `remote_root` en paralelo, podando subárboles fuera del rango.

    Muestra un spinner con el conteo de carpetas exploradas, podadas y
    archivos coincidentes.
    """
    matched: list[RemoteEntry] = []
    dirs_explored = 0
    range_str = date_filter.describe()
    start = date_filter.start_date
    end = date_filter.end_date
    prune = _yyyymmdd_prune_predicate(date_filter)

    def status_text() -> str:
        pruned = walker.pruned_count
        extra = f", {pruned} podadas" if pruned else ""
        return (
            f"[cyan]Buscando archivos del {range_str}...[/] "
            f"[dim]({dirs_explored} carpetas{extra}, "
            f"{len(matched)} coincidencias)[/]"
        )

    with renderer.console.status(
        f"[cyan]Buscando archivos del {range_str}...[/]", spinner="dots"
    ) as status:
        for entry in walker.walk(remote_root, errors=errors, prune=prune):
            if entry.is_dir:
                dirs_explored += 1
                status.update(status_text())
            elif start <= entry.modified_at.date() <= end:
                matched.append(entry)
                status.update(status_text())

    pruned_total = walker.pruned_count
    pruned_msg = f", {pruned_total} podada(s) por nombre" if pruned_total else ""
    renderer.success(
        f"Exploración terminada: {dirs_explored} carpeta(s){pruned_msg}, "
        f"{len(matched)} archivo(s) del {range_str}."
    )
    return matched


def main() -> int:
    renderer = ConsoleRenderer()

    # Cargar configuración antes de la bienvenida para fallar rápido.
    try:
        config = EnvConfigLoader().load()
    except ConfigError as e:
        renderer.error("Problema con la configuración (.env):", str(e))
        return 2

    renderer.welcome()

    try:
        with SFTPConnectionService(config) as connection:
            # ── Paso 1: conexión + diagnóstico ──
            renderer.step(1, "Conectando al servidor")
            renderer.info(f"Conectando a [bold]{config.credentials.host}[/]...")
            diagnostic = ConnectionDiagnosticService().run(
                connection.client, connection.server_banner
            )
            if not diagnostic.ok:
                renderer.error(
                    "No pudimos conectar al servidor.",
                    diagnostic.error_message or "Razón desconocida.",
                )
                return 3
            renderer.success(
                f"Conexión exitosa (latencia: {diagnostic.latency_ms:.0f} ms)"
            )

            listing_service = RemoteListingService(connection.client)
            prompt_service = UserPromptService(renderer, listing_service)

            # ── Paso 2: navegar a la carpeta ──
            renderer.step(2, "Elegir carpeta a descargar")
            renderer.console.print(
                "Navega entre carpetas. Cuando estés en la que quieres,"
            )
            renderer.console.print("escribe 'd' para elegirla.")
            remote_root = prompt_service.navigate_to_folder()
            renderer.success(
                f"Carpeta elegida: [bold]"
                f"{'/' if remote_root == '.' else remote_root}[/]"
            )

            # ── Paso 3: elegir rango de fechas ──
            renderer.step(3, "Elegir rango de fechas")
            display_root = "/" if remote_root == "." else remote_root
            renderer.console.print(
                "Vamos a descargar todos los archivos dentro de"
            )
            renderer.console.print(
                f'"[bold]{display_root}[/]" (incluyendo subcarpetas) que'
            )
            renderer.console.print(
                "fueron subidos al servidor entre las fechas que indiques."
            )
            renderer.console.print()

            walker = ParallelRemoteWalker(connection, config.max_workers)
            walk_errors: list[str] = []
            while True:
                date_filter = prompt_service.ask_date_range()
                walk_errors = []
                filtered = _walk_filter_with_progress(
                    walker,
                    remote_root,
                    date_filter,
                    renderer,
                    walk_errors,
                )
                if filtered:
                    break
                renderer.console.print()
                renderer.warning(
                    f"No encontramos archivos del rango "
                    f"{date_filter.describe()} en esa carpeta."
                )
                if not prompt_service.confirm("¿Probar con otro rango?", default=True):
                    renderer.goodbye()
                    return 0
                renderer.console.print()

            if walk_errors:
                renderer.warning(
                    f"{len(walk_errors)} subcarpeta(s) no se pudieron leer "
                    "(probablemente por permisos). Los archivos dentro NO se "
                    "incluirán en la descarga."
                )
                shown = walk_errors[:3]
                for p in shown:
                    renderer.info(f"  • {p}")
                if len(walk_errors) > len(shown):
                    renderer.info(f"  … y {len(walk_errors) - len(shown)} más")

            # ── Paso 4: confirmar ──
            renderer.step(4, "Confirmar descarga")
            date_str = date_filter.folder_name()
            if remote_root == ".":
                folder_basename = "raiz"
            else:
                folder_basename = (
                    posixpath.basename(remote_root.rstrip("/")) or "raiz"
                )
            local_dest = config.local_download_root / date_str / folder_basename
            total_bytes = sum(e.size_bytes for e in filtered)
            plan = DownloadPlan(
                remote_root=remote_root,
                entries=sorted(filtered, key=lambda x: x.full_path),
                local_destination=local_dest,
                total_bytes=total_bytes,
            )

            renderer.preview_panel(plan, date_filter)

            # Advertencia de rutas largas en Windows.
            if sys.platform == "win32":
                longest = 0
                for e in plan.entries:
                    candidate = DownloadService._compute_local_path(e, plan)
                    longest = max(longest, len(str(candidate)))
                if longest > 260:
                    renderer.warning(
                        f"Algunas rutas de destino superan 260 caracteres "
                        f"(la más larga: {longest}). Si Windows no tiene habilitadas "
                        "las 'long paths', algunas descargas podrían fallar."
                    )
                    renderer.console.print()

            if not prompt_service.confirm("¿Comenzar la descarga?", default=True):
                renderer.console.print("[dim]Descarga cancelada.[/]")
                renderer.goodbye()
                return 0

            # ── Descargar ──
            summary = DownloadService(connection, config, renderer).run(plan)
            renderer.summary_panel(
                summary, plan.local_destination, walk_errors=walk_errors
            )
            return 0 if summary.failures == 0 else 1

    except KeyboardInterrupt:
        renderer.console.print("\n[yellow]Cancelado por el usuario.[/]")
        return 130
    except AuthenticationException as e:
        renderer.error("No pudimos autenticarnos.", ErrorTranslator.translate(e))
        return 3
    except (socket.gaierror, socket.timeout, SSHException) as e:
        renderer.error("Problema de conexión.", ErrorTranslator.translate(e))
        return 3
    except Exception as e:
        renderer.error(
            "Ocurrió un error inesperado.", ErrorTranslator.translate(e)
        )
        return 4


if __name__ == "__main__":
    sys.exit(main())
