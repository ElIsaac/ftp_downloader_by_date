<#
.SYNOPSIS
  Bootstrap del descargador SFTP por fecha.

.DESCRIPTION
  Descarga el repositorio como ZIP, crea un entorno virtual de Python,
  instala/valida las dependencias y lanza el script.

  Compatible con Windows PowerShell 5.1, PowerShell 7.x y PowerShell 8.
  No requiere git ni administrador.

.PARAMETER InstallDir
  Carpeta donde se instala el proyecto. Por defecto: %LOCALAPPDATA%\sftp_downloader

.PARAMETER Branch
  Rama del repo a descargar. Por defecto: main

.PARAMETER SkipRun
  Solo prepara el entorno; no ejecuta el script al final.

.EXAMPLE
  # Instalación + ejecución (modo normal)
  powershell -ExecutionPolicy Bypass -File install.ps1

  # Desde PowerShell 7+
  pwsh -ExecutionPolicy Bypass -File install.ps1

  # One-liner que descarga y corre este mismo instalador
  iex (irm https://raw.githubusercontent.com/ElIsaac/ftp_downloader_by_date/main/install.ps1)
#>

[CmdletBinding()]
param(
    [string]$InstallDir = (Join-Path $env:LOCALAPPDATA 'sftp_downloader'),
    [string]$RepoOwner  = 'ElIsaac',
    [string]$RepoName   = 'ftp_downloader_by_date',
    [string]$Branch     = 'main',
    [switch]$SkipRun
)

$ErrorActionPreference = 'Stop'
$ProgressPreference    = 'SilentlyContinue'   # Acelera Invoke-WebRequest en WinPS 5.1

# ─────────────────────────────────────────────────────────────────────────────
# Helpers de impresión
# ─────────────────────────────────────────────────────────────────────────────
function Write-Step  { param($Msg) Write-Host "`n==> $Msg" -ForegroundColor Cyan }
function Write-OK    { param($Msg) Write-Host "  [OK] $Msg" -ForegroundColor Green }
function Write-Warn2 { param($Msg) Write-Host "  [!]  $Msg" -ForegroundColor Yellow }
function Write-Err2  { param($Msg) Write-Host "  [X]  $Msg" -ForegroundColor Red }

# ─────────────────────────────────────────────────────────────────────────────
# Detectar Python (3.9+). Prefiere "py -3" en Windows, cae a python/python3.
# ─────────────────────────────────────────────────────────────────────────────
function Find-Python {
    $candidates = @(
        [pscustomobject]@{ Cmd = 'py';      Prefix = @('-3') }
        [pscustomobject]@{ Cmd = 'python';  Prefix = @() }
        [pscustomobject]@{ Cmd = 'python3'; Prefix = @() }
    )
    foreach ($c in $candidates) {
        $found = Get-Command $c.Cmd -ErrorAction SilentlyContinue
        if (-not $found) { continue }
        try {
            $vArgs = @() + $c.Prefix + @('--version')
            $vOut  = (& $found.Source @vArgs 2>&1) | Out-String
            if ($LASTEXITCODE -eq 0 -and $vOut -match 'Python\s+(\d+)\.(\d+)') {
                $maj = [int]$Matches[1]; $min = [int]$Matches[2]
                if ($maj -eq 3 -and $min -ge 9) {
                    return [pscustomobject]@{
                        Exe     = $found.Source
                        Prefix  = $c.Prefix
                        Version = "$maj.$min"
                    }
                }
            }
        } catch { }
    }
    return $null
}

function Invoke-PythonHost {
    param(
        [Parameter(Mandatory)] $Spec,
        [Parameter(Mandatory)][string[]] $ScriptArgs
    )
    $all = @() + $Spec.Prefix + $ScriptArgs
    & $Spec.Exe @all
}

# ─────────────────────────────────────────────────────────────────────────────
# 1. Python
# ─────────────────────────────────────────────────────────────────────────────
Write-Step 'Detectando Python'
$py = Find-Python
if (-not $py) {
    Write-Err2 'No encontré Python 3.9+ en este sistema.'
    Write-Host '       Instálalo desde https://www.python.org/downloads/'
    Write-Host '       (marca "Add Python to PATH" durante la instalación).'
    exit 1
}
Write-OK "Python $($py.Version) en $($py.Exe)"

# ─────────────────────────────────────────────────────────────────────────────
# 2. TLS 1.2 — necesario en Windows PowerShell 5.1 (default a veces es TLS 1.0)
# ─────────────────────────────────────────────────────────────────────────────
if ($PSVersionTable.PSVersion.Major -lt 6) {
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    } catch {
        Write-Warn2 'No pude forzar TLS 1.2 — la descarga puede fallar en Windows antiguo.'
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# 3. Descargar el ZIP
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Descargando $RepoOwner/$RepoName ($Branch)"
$zipUrl     = "https://github.com/$RepoOwner/$RepoName/archive/refs/heads/$Branch.zip"
$tmpBase    = [IO.Path]::GetTempPath()
$tmpZip     = Join-Path $tmpBase ("{0}_{1}.zip" -f $RepoName, [guid]::NewGuid().Guid)
$tmpExtract = Join-Path $tmpBase ("{0}_{1}"    -f $RepoName, [guid]::NewGuid().Guid)

try {
    Invoke-WebRequest -Uri $zipUrl -OutFile $tmpZip -UseBasicParsing
} catch {
    Write-Err2 "Falló la descarga: $($_.Exception.Message)"
    Write-Host '       Verifica tu conexión a internet y que el repositorio sea público.'
    exit 2
}
Write-OK "ZIP descargado"

# ─────────────────────────────────────────────────────────────────────────────
# 4. Extraer
# ─────────────────────────────────────────────────────────────────────────────
Write-Step 'Extrayendo ZIP'
try {
    Expand-Archive -LiteralPath $tmpZip -DestinationPath $tmpExtract -Force
} catch {
    Write-Err2 "Falló la extracción: $($_.Exception.Message)"
    exit 3
}
$srcRoot = Join-Path $tmpExtract "$RepoName-$Branch"
if (-not (Test-Path $srcRoot)) {
    Write-Err2 "No encontré la carpeta esperada dentro del ZIP: $srcRoot"
    exit 3
}
Write-OK 'Extraído'

# ─────────────────────────────────────────────────────────────────────────────
# 5. Sincronizar a $InstallDir (sin sobrescribir .env existente)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Instalando en $InstallDir"
if (-not (Test-Path $InstallDir)) {
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
}
Get-ChildItem -Path $srcRoot -Force | ForEach-Object {
    $dest = Join-Path $InstallDir $_.Name
    if ($_.Name -eq '.env' -and (Test-Path $dest)) {
        Write-Warn2 'Conservo el .env existente (no lo sobrescribo).'
        return
    }
    Copy-Item -LiteralPath $_.FullName -Destination $dest -Recurse -Force
}
Write-OK 'Archivos sincronizados'

Remove-Item -LiteralPath $tmpZip     -Force            -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $tmpExtract -Recurse -Force   -ErrorAction SilentlyContinue

# ─────────────────────────────────────────────────────────────────────────────
# 6. requirements.txt — crear si falta o si le faltan paquetes
# ─────────────────────────────────────────────────────────────────────────────
Write-Step 'Verificando requirements.txt'
$reqPath     = Join-Path $InstallDir 'requirements.txt'
$requiredPkgs = @('paramiko', 'python-dotenv', 'rich')
$defaultReqs  = @('paramiko>=3.0', 'python-dotenv>=1.0', 'rich>=13.0')
$needsRegen  = $false

if (-not (Test-Path $reqPath)) {
    Write-Warn2 'requirements.txt no estaba — lo creo.'
    $needsRegen = $true
} else {
    $content = Get-Content -LiteralPath $reqPath -Raw -ErrorAction SilentlyContinue
    if ([string]::IsNullOrWhiteSpace($content)) {
        $needsRegen = $true
    } else {
        foreach ($pkg in $requiredPkgs) {
            if ($content -notmatch [regex]::Escape($pkg)) {
                Write-Warn2 "Falta '$pkg' en requirements.txt — lo regenero."
                $needsRegen = $true
                break
            }
        }
    }
}
if ($needsRegen) {
    Set-Content -LiteralPath $reqPath -Value $defaultReqs -Encoding UTF8
}
Write-OK 'requirements.txt OK'

# ─────────────────────────────────────────────────────────────────────────────
# 7. Crear venv
# ─────────────────────────────────────────────────────────────────────────────
$venvDir    = Join-Path $InstallDir '.venv'
$venvPython = Join-Path $venvDir 'Scripts\python.exe'

Write-Step 'Preparando entorno virtual'
if (-not (Test-Path $venvPython)) {
    Invoke-PythonHost -Spec $py -ScriptArgs @('-m', 'venv', $venvDir)
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $venvPython)) {
        Write-Err2 'No se pudo crear el venv.'
        exit 4
    }
    Write-OK "venv creado en $venvDir"
} else {
    Write-OK 'venv ya existe'
}

# ─────────────────────────────────────────────────────────────────────────────
# 8. Instalar dependencias
# ─────────────────────────────────────────────────────────────────────────────
Write-Step 'Instalando dependencias (pip)'
& $venvPython -m pip install --upgrade pip --disable-pip-version-check --quiet
if ($LASTEXITCODE -ne 0) { Write-Warn2 'No se pudo actualizar pip (continúo).' }

& $venvPython -m pip install -r $reqPath --disable-pip-version-check
if ($LASTEXITCODE -ne 0) {
    Write-Err2 'Falló la instalación de dependencias.'
    exit 5
}
Write-OK 'Dependencias instaladas'

# ─────────────────────────────────────────────────────────────────────────────
# 9. Re-validar: importar paquetes desde el venv
# ─────────────────────────────────────────────────────────────────────────────
Write-Step 'Validando dependencias'
$probe = & $venvPython -c "import paramiko, dotenv, rich; print('OK')" 2>&1
$probeStr = ($probe | Out-String).Trim()
if ($LASTEXITCODE -ne 0 -or $probeStr -notmatch 'OK') {
    Write-Err2 "Validación falló: $probeStr"
    exit 6
}
Write-OK 'paramiko, python-dotenv, rich importan correctamente'

# ─────────────────────────────────────────────────────────────────────────────
# 10. .env — copiar de .env.example si no existe y ofrecer editarlo
# ─────────────────────────────────────────────────────────────────────────────
$envPath    = Join-Path $InstallDir '.env'
$envExample = Join-Path $InstallDir '.env.example'

if (-not (Test-Path $envPath)) {
    if (Test-Path $envExample) {
        Write-Warn2 '.env no existe. Copiando desde .env.example.'
        Copy-Item -LiteralPath $envExample -Destination $envPath
        Write-Host "       Edita el archivo con tus credenciales:" -ForegroundColor Yellow
        Write-Host "       $envPath"                                -ForegroundColor Yellow
        $r = Read-Host '       ¿Abrir Notepad para editarlo ahora? [S/n]'
        if ($r -notmatch '^[nN]') {
            Start-Process -FilePath 'notepad.exe' -ArgumentList $envPath -Wait
        }
    } else {
        Write-Warn2 'No hay .env ni .env.example. El script pedirá uno al arrancar.'
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# 11. Lanzar el script (a menos que -SkipRun)
# ─────────────────────────────────────────────────────────────────────────────
$mainScript = Join-Path $InstallDir 'sftp_downloader.py'
if (-not (Test-Path $mainScript)) {
    Write-Err2 "No encontré sftp_downloader.py en $InstallDir"
    exit 7
}

if ($SkipRun) {
    Write-Step 'Listo. (-SkipRun activo, no ejecuto el script).'
    Write-Host  '  Para ejecutarlo manualmente:'
    Write-Host  "    `"$venvPython`" `"$mainScript`""
    exit 0
}

Write-Step 'Iniciando descargador SFTP'
Push-Location $InstallDir
try {
    & $venvPython $mainScript
    $rc = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $rc
