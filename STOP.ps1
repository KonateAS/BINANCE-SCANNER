$ErrorActionPreference = 'Continue'
$root = $PSScriptRoot
$runtime = Join-Path $root '.runtime'
$failures = @()

function Get-OwnedProcess([string]$Name) {
  $file = Join-Path $runtime "$Name.pid.json"
  if (-not (Test-Path $file)) { return $null }
  try {
    $record = Get-Content $file -Raw | ConvertFrom-Json
    $process = Get-Process -Id ([int]$record.pid) -ErrorAction Stop
    $expected = [DateTime]::Parse($record.startedUtc).ToUniversalTime()
    if ([Math]::Abs(($process.StartTime.ToUniversalTime() - $expected).TotalSeconds) -gt 3) {
      Write-Warning "Ignoring stale $Name PID file; process id has been reused."
      Remove-Item $file -Force
      return $null
    }
    return $process
  } catch {
    Remove-Item $file -Force -ErrorAction SilentlyContinue
    return $null
  }
}
function Stop-OwnedProcess([string]$Name) {
  $process = Get-OwnedProcess $Name
  if (-not $process) { return }
  $marker = Join-Path $runtime "$Name.stop"
  New-Item -ItemType File -Force -Path $marker | Out-Null
  $deadline = (Get-Date).AddSeconds(45)
  while ((Get-Date) -lt $deadline) {
    try { $process.Refresh(); if ($process.HasExited) { break } } catch { break }
    Start-Sleep -Milliseconds 250
  }
  try { $process.Refresh() } catch { }
  if (-not $process.HasExited) {
    Write-Warning "$Name did not stop gracefully within 45 seconds; terminating its process."
    try { Stop-Process -Id $process.Id -Force -ErrorAction Stop } catch { $script:failures += "Could not stop $Name PID $($process.Id): $_" }
  } else {
    Write-Host "$Name stopped cleanly."
  }
  Remove-Item (Join-Path $runtime "$Name.pid.json") -Force -ErrorAction SilentlyContinue
  Remove-Item $marker -Force -ErrorAction SilentlyContinue
}

Stop-OwnedProcess 'frontend'
Stop-OwnedProcess 'backend'

$pgStarted = Join-Path $runtime 'postgres.started'
if (Test-Path $pgStarted) {
  $data = Get-Content $pgStarted -Raw
  $pgCtl = 'C:\Program Files\PostgreSQL\17\bin\pg_ctl.exe'
  if (Test-Path $pgCtl) {
    & $pgCtl stop -D $data -m fast -w
    if ($LASTEXITCODE -eq 0) {
      Write-Host 'PostgreSQL started by this run stopped cleanly.'
      Remove-Item $pgStarted -Force
    } else { $failures += 'Could not stop the PostgreSQL cluster started by START.cmd.' }
  } else { $failures += "pg_ctl.exe not found; PostgreSQL remains running ($data)." }
}
$pgServiceStarted = Join-Path $runtime 'postgres.service.started'
if (Test-Path $pgServiceStarted) {
  $serviceName = Get-Content $pgServiceStarted -Raw
  try {
    Stop-Service -Name $serviceName -ErrorAction Stop
    Write-Host 'PostgreSQL service started by this run stopped.'
    Remove-Item $pgServiceStarted -Force
  } catch { $failures += "Could not stop PostgreSQL service '$serviceName'. $($_.Exception.Message)" }
}

if ($failures.Count) {
  $failures | ForEach-Object { Write-Error $_ }
  exit 1
}
Write-Host 'Scanner processes started by START.cmd have been stopped.'