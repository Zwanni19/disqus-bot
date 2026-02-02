#requires -Version 7.0
[CmdletBinding()]
param(
  [ValidateSet('Startup','Shutdown','Restart','Menu')]
  [string]$Action = 'Menu',

  [int]$StartupDelaySeconds = 1,

  [switch]$PreferVenv = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$BaseDir = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($BaseDir)) {
  $BaseDir = (Split-Path -Parent $MyInvocation.MyCommand.Path)
}
Set-Location -LiteralPath $BaseDir

$PidDir    = Join-Path $BaseDir '.pids'
$RunnerDir = Join-Path $BaseDir '.runners'
$LogDir    = Join-Path $BaseDir '.logs'
$LogFile   = Join-Path $LogDir  'control.log'

$MockName   = 'mock_api'
$BotName    = 'bot'
$MockScript = Join-Path $BaseDir 'mock_api.py'
$BotScript  = Join-Path $BaseDir 'bot.py'

New-Item -ItemType Directory -Path $PidDir    -Force | Out-Null
New-Item -ItemType Directory -Path $RunnerDir -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir    -Force | Out-Null

function Write-Log {
  param(
    [Parameter(Mandatory)][string]$Message,
    [ValidateSet('INFO','WARN','ERROR')][string]$Level = 'INFO'
  )
  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  $line = "[${ts}][${Level}] ${Message}"
  Write-Host $line
  Add-Content -Path $LogFile -Value $line
}

function Get-PidFilePath {
  param([Parameter(Mandatory)][string]$Name)
  Join-Path $PidDir ("{0}.pid" -f $Name)
}

function Read-PidFile {
  param([Parameter(Mandatory)][string]$Name)
  $pidFile = Get-PidFilePath -Name $Name
  if (-not (Test-Path -LiteralPath $pidFile)) { return $null }
  $raw = (Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
  if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
  if ($raw -match '^\d+$') { return [int]$raw }
  return $null
}

function Remove-PidFile {
  param([Parameter(Mandatory)][string]$Name)
  $pidFile = Get-PidFilePath -Name $Name
  if (Test-Path -LiteralPath $pidFile) {
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
  }
}

function Test-ProcessAlive {
  param([Parameter(Mandatory)][int]$ProcId)
  try { Get-Process -Id $ProcId -ErrorAction Stop | Out-Null; return $true } catch { return $false }
}

function Stop-ByPidFile {
  param([Parameter(Mandatory)][string]$Name)

  $procId = Read-PidFile -Name $Name
  if (-not $procId) {
    Write-Log ("{0}: no PID file found." -f $Name) 'WARN'
    return
  }

  if (-not (Test-ProcessAlive -ProcId $procId)) {
    Write-Log ("{0}: PID {1} not running; cleaning PID file." -f $Name, $procId) 'WARN'
    Remove-PidFile -Name $Name
    return
  }

  Write-Log ("{0}: stopping PID {1} ..." -f $Name, $procId) 'INFO'
  try {
    Stop-Process -Id $procId -Force -ErrorAction Stop
    Start-Sleep -Milliseconds 300
  } catch {
    Write-Log ("{0}: failed to stop PID {1}: {2}" -f $Name, $procId, $_.Exception.Message) 'ERROR'
  }

  if (-not (Test-ProcessAlive -ProcId $procId)) {
    Remove-PidFile -Name $Name
    Write-Log ("{0}: stopped." -f $Name) 'INFO'
  } else {
    Write-Log ("{0}: still running after stop attempt." -f $Name) 'WARN'
  }
}

function Resolve-PythonExePath {
  if ($PreferVenv) {
    $venvPy = Join-Path $BaseDir '.venv\Scripts\python.exe'
    if (Test-Path -LiteralPath $venvPy) { return $venvPy }
  }

  $pyCmd = Get-Command -Name 'py' -ErrorAction SilentlyContinue
  if ($pyCmd -and $pyCmd.CommandType -eq 'Application') {
    try {
      $resolved = (& $pyCmd.Source -c "import sys; print(sys.executable)" 2>$null | Select-Object -First 1).Trim()
      if (-not [string]::IsNullOrWhiteSpace($resolved) -and (Test-Path -LiteralPath $resolved)) {
        return $resolved
      }
    } catch { }
  }

  $pythonCmd = Get-Command -Name 'python' -ErrorAction SilentlyContinue
  if ($pythonCmd -and $pythonCmd.CommandType -eq 'Application') {
    return $pythonCmd.Source
  }

  throw "No runnable python.exe found. Install Python or ensure python.exe is on PATH (or create .\.venv)."
}

function New-RunnerScript {
  param(
    [Parameter(Mandatory)][string]$Title,
    [Parameter(Mandatory)][string]$Name,
    [Parameter(Mandatory)][string]$PythonExe,
    [Parameter(Mandatory)][string]$ScriptPath
  )

  $pidFile = Get-PidFilePath -Name $Name
  $runner  = Join-Path $RunnerDir ("run_{0}.ps1" -f $Name)

  $escapedBase   = $BaseDir.Replace("'", "''")
  $escapedPidDir = $PidDir.Replace("'", "''")
  $escapedPy     = $PythonExe.Replace("'", "''")
  $escapedScript = $ScriptPath.Replace("'", "''")
  $escapedPid    = $pidFile.Replace("'", "''")

  $content = @"
Set-StrictMode -Version Latest
`$ErrorActionPreference = 'Stop'

Set-Location -LiteralPath '$escapedBase'
New-Item -ItemType Directory -Path '$escapedPidDir' -Force | Out-Null

`$env:PYTHONUNBUFFERED = '1'

Write-Host "[$Title] Python: $escapedPy"
Write-Host "[$Title] Script: $escapedScript"
Write-Host "[$Title] Starting ..."

try {
  `$proc = Start-Process -FilePath '$escapedPy' -ArgumentList @('-u', '$escapedScript') -PassThru -NoNewWindow
  Set-Content -Path '$escapedPid' -Value `$proc.Id -Encoding ascii
  Write-Host "[$Title] PID: `$(`$proc.Id) (PID file: $escapedPid)"
  Write-Host "Close window or Ctrl+C to stop this window; control.ps1 Shutdown also works."
  Wait-Process -Id `$proc.Id
} catch {
  Write-Host "[$Title] ERROR: `$(`$_.Exception.Message)"
  Write-Host 'Press Enter to close this tab...'
  [void](Read-Host)
}
"@


  Set-Content -LiteralPath $runner -Value $content -Encoding UTF8
  return $runner
}

function Start-InWindowsTerminalTab {
  param(
    [Parameter(Mandatory)][string]$Title,
    [Parameter(Mandatory)][string]$RunnerPath
  )

  $wt = Get-Command -Name 'wt.exe' -ErrorAction SilentlyContinue
  if (-not $wt) { return $false }

  $args = @(
    '-w','0',
    'new-tab',
    '--title', $Title,
    'pwsh', '-NoExit', '-ExecutionPolicy', 'Bypass', '-File', $RunnerPath
  )

  Start-Process -FilePath $wt.Source -ArgumentList $args -WindowStyle Normal | Out-Null
  return $true
}

function Start-InConHostWindow {
  param(
    [Parameter(Mandatory)][string]$Title,
    [Parameter(Mandatory)][string]$RunnerPath
  )

  Start-Process -FilePath 'cmd.exe' -ArgumentList @(
    '/c','start', "`"$Title`"", 'pwsh', '-NoExit', '-ExecutionPolicy','Bypass','-File', "`"$RunnerPath`""
  ) -WindowStyle Normal | Out-Null
}

function Start-PythonTab {
  param(
    [Parameter(Mandatory)][string]$Title,
    [Parameter(Mandatory)][string]$Name,
    [Parameter(Mandatory)][string]$PythonExe,
    [Parameter(Mandatory)][string]$ScriptPath
  )

  if (-not (Test-Path -LiteralPath $ScriptPath)) {
    throw ("Script not found: {0}" -f $ScriptPath)
  }

  $existingProcId = Read-PidFile -Name $Name
  if ($existingProcId -and (Test-ProcessAlive -ProcId $existingProcId)) {
    Write-Log ("{0}: already running (PID {1}). Skipping start." -f $Title, $existingProcId) 'WARN'
    return
  } elseif ($existingProcId) {
    Remove-PidFile -Name $Name
  }

  $runner = New-RunnerScript -Title $Title -Name $Name -PythonExe $PythonExe -ScriptPath $ScriptPath

  if (-not (Start-InWindowsTerminalTab -Title $Title -RunnerPath $runner)) {
    Start-InConHostWindow -Title $Title -RunnerPath $runner
  }

  Write-Log ("{0}: launch requested (runner: {1})" -f $Title, $runner) 'INFO'
}

function Start-All {
  $pyExe = Resolve-PythonExePath
  Write-Log ("Resolved python.exe: {0}" -f $pyExe) 'INFO'

  Start-PythonTab -Title 'mock_api.py' -Name $MockName -PythonExe $pyExe -ScriptPath $MockScript
  Start-Sleep -Seconds $StartupDelaySeconds
  Start-PythonTab -Title 'bot.py'      -Name $BotName  -PythonExe $pyExe -ScriptPath $BotScript
}

function Stop-All {
  Stop-ByPidFile -Name $BotName
  Stop-ByPidFile -Name $MockName
}

function Restart-All {
  Stop-All
  Start-Sleep -Seconds 1
  Start-All
}

function Show-MenuPopup {
  Add-Type -AssemblyName System.Windows.Forms

  $msg = @"
Choose an action:

Yes    = Startup
No     = Shutdown
Cancel = Restart
"@

  $result = [System.Windows.Forms.MessageBox]::Show(
    $msg,
    'control.ps1',
    [System.Windows.Forms.MessageBoxButtons]::YesNoCancel,
    [System.Windows.Forms.MessageBoxIcon]::Question
  )

  switch ($result) {
    'Yes'    { 'Startup' }
    'No'     { 'Shutdown' }
    'Cancel' { 'Restart' }
    default  { 'Menu' }
  }
}

function Relaunch-WithBypassIfNeeded {
  if ($env:CONTROLPS1_RELAUNCHED -eq '1') { return }
  $env:CONTROLPS1_RELAUNCHED = '1'

  try { $ep = Get-ExecutionPolicy -Scope Process } catch { $ep = 'Unknown' }
  if ($ep -in @('Restricted','AllSigned','RemoteSigned','Undefined')) {
    $self = $MyInvocation.MyCommand.Path
    Start-Process -FilePath 'pwsh.exe' -ArgumentList @(
      '-NoProfile','-ExecutionPolicy','Bypass','-File', $self, '-Action', $Action
    ) -WorkingDirectory $BaseDir | Out-Null
    exit
  }
}

Relaunch-WithBypassIfNeeded

try {
  if ($Action -eq 'Menu') { $Action = Show-MenuPopup }

  Write-Log ("Action selected: {0}" -f $Action) 'INFO'

  switch ($Action) {
    'Startup'  { Start-All }
    'Shutdown' { Stop-All }
    'Restart'  { Restart-All }
    default    { Write-Log ("Unknown action: {0}" -f $Action) 'ERROR' }
  }
} catch {
  Write-Log ("Fatal: {0}" -f $_.Exception.Message) 'ERROR'
  throw
}
