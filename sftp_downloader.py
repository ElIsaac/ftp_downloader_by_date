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
import socket
import stat as stat_mod
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Iterator

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
    target_date: date


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
                return "No tienes permiso de escribir en la carpeta de destino."
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

    def walk(
        self, path: str, errors: list[str] | None = None
    ) -> Iterator[RemoteEntry]:
        """Recorre `path` recursivamente.

        Si `errors` se proporciona, los subdirectorios que no se pudieron leer
        (típicamente PermissionError) se acumulan ahí para que el caller pueda
        avisar al usuario.
        """
        stack = [path]
        while stack:
            current = stack.pop()
            try:
                attrs_list = self._client.listdir_attr(current)
            except OSError:
                if errors is not None:
                    errors.append(current)
                continue
            for attr in attrs_list:
                entry = self._to_entry(attr, current)
                yield entry
                if entry.is_dir:
                    stack.append(entry.full_path)

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
            if not e.is_dir and e.modified_at.date() == date_filter.target_date
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

    def listing_table(self, entries: list[RemoteEntry], max_rows: int = 30) -> None:
        table = Table(show_header=True, header_style="bold", box=None, pad_edge=False)
        table.add_column("Tipo", width=6)
        table.add_column("Nombre", overflow="fold")
        table.add_column("Modificado", width=12)

        sorted_entries = sorted(
            entries, key=lambda x: (not x.is_dir, x.name.lower())
        )
        shown = sorted_entries[:max_rows]
        for e in shown:
            icon = "📁" if e.is_dir else "📄"
            table.add_row(icon, e.name, e.modified_at.strftime("%d-%m-%Y"))
        self.console.print(table)
        remaining = len(sorted_entries) - len(shown)
        if remaining > 0:
            self.console.print(f"  [dim]… y {remaining} más[/]")

    def preview_panel(self, plan: DownloadPlan, target_date: date) -> None:
        fecha_human = format_date_human(target_date)
        body = (
            f"  Carpeta origen:  [bold]{plan.remote_root}[/]\n"
            f"  Fecha:           [bold]{target_date.strftime('%d-%m-%Y')}[/]\n"
            f"                   [dim]({fecha_human})[/]\n\n"
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

    def summary_panel(self, summary: DownloadSummary, destination: Path) -> None:
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

    def ask_relative_path(self) -> str:
        c = self._renderer.console
        c.print("Escribe la ruta de la carpeta cuyo contenido")
        c.print("quieres descargar. Puedes usar subcarpetas.")
        c.print()
        c.print("  [dim]Ejemplos:  facturas[/]")
        c.print("  [dim]           facturas/2026[/]")
        c.print("  [dim]           reportes/mensuales/mayo[/]")
        c.print()

        while True:
            raw = self._renderer.prompt("Ruta")
            normalized = raw.strip().strip("/").replace("\\", "/")
            if not normalized:
                self._renderer.error(
                    "La ruta no puede estar vacía.",
                    "Escribe el nombre de una carpeta como aparece en la lista de arriba.",
                )
                continue
            if self._listing.validate_path(normalized):
                return normalized
            self._renderer.error(
                f'La carpeta "{normalized}" no existe en el servidor.',
                "Revisa que esté escrita exactamente como en el listado e inténtalo de nuevo.",
            )

    def ask_target_date(self) -> DateFilter:
        c = self._renderer.console
        c.print("Formato: [bold]DD-MM-AAAA[/]  (día-mes-año, con guiones)")
        c.print("[dim]Ejemplo: 15-05-2026[/]")
        c.print()

        while True:
            raw = self._renderer.prompt("Fecha")
            try:
                parsed = DateParser.parse(raw)
                return DateFilter(target_date=parsed)
            except ValueError:
                self._renderer.error(
                    "Formato incorrecto. Necesito DD-MM-AAAA.",
                    "Ejemplo válido: 15-05-2026. Inténtalo otra vez.",
                )

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

            # ── Paso 2: elegir carpeta ──
            renderer.step(2, "Elegir carpeta a explorar")
            root_entries = listing_service.list_dir(".")
            renderer.console.print("Contenido de la raíz del servidor:")
            renderer.console.print()
            renderer.listing_table(root_entries)
            renderer.console.print()

            remote_root = prompt_service.ask_relative_path()

            renderer.info("Explorando subcarpetas...")
            walk_errors: list[str] = []
            all_entries = list(
                listing_service.walk(remote_root, errors=walk_errors)
            )
            files_count = sum(1 for e in all_entries if not e.is_dir)
            dirs_count = sum(1 for e in all_entries if e.is_dir)
            renderer.success(f"Encontrada: [bold]{remote_root}[/]")
            renderer.info(
                f"Contiene {dirs_count} subcarpetas y {files_count} archivos."
            )

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

            if files_count == 0:
                renderer.warning(
                    "Esta carpeta no contiene archivos. No hay nada que descargar."
                )
                renderer.goodbye()
                return 0

            # ── Paso 3: elegir fecha ──
            renderer.step(3, "Elegir fecha de los archivos")
            renderer.console.print(
                "Vamos a descargar todos los archivos dentro de"
            )
            renderer.console.print(
                f'"[bold]{remote_root}[/]" (incluyendo subcarpetas) que'
            )
            renderer.console.print(
                "fueron subidos al servidor en una fecha específica."
            )
            renderer.console.print()

            while True:
                date_filter = prompt_service.ask_target_date()
                filtered = DateFilterService().apply(all_entries, date_filter)
                if filtered:
                    break
                renderer.console.print()
                renderer.warning(
                    f"No encontramos archivos del "
                    f"{date_filter.target_date.strftime('%d-%m-%Y')} en esa carpeta."
                )
                if not prompt_service.confirm("¿Probar con otra fecha?", default=True):
                    renderer.goodbye()
                    return 0
                renderer.console.print()

            # ── Paso 4: confirmar ──
            renderer.step(4, "Confirmar descarga")
            date_str = date_filter.target_date.strftime("%d-%m-%Y")
            folder_basename = posixpath.basename(remote_root.rstrip("/")) or "raiz"
            local_dest = config.local_download_root / date_str / folder_basename
            total_bytes = sum(e.size_bytes for e in filtered)
            plan = DownloadPlan(
                remote_root=remote_root,
                entries=sorted(filtered, key=lambda x: x.full_path),
                local_destination=local_dest,
                total_bytes=total_bytes,
            )

            renderer.preview_panel(plan, date_filter.target_date)

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
            renderer.summary_panel(summary, plan.local_destination)
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
