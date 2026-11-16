$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$runtime = Join-Path $root '.runtime'
$backend = Join-Path $root 'backend'
$frontend = Join-Path $root 'frontend'
$python = Join-Path $root '.venv\Scripts\python.exe'
$node = (Get-Command node.exe -ErrorAction SilentlyContinue).Source
$pgData = Join-Path $env:LOCALAPPDATA 'BinanceScanner\PostgreSQL\17\data'
$pgCtl = 'C:\Program Files\PostgreSQL\17\bin\pg_ctl.exe'
New-Item -ItemType Directory -Force -Path $runtime | Out-Null
$started = @()

function Test-TcpPort([int]$Port) {
  $client = [System.Net.Sockets.TcpClient]::new()
  try {
    $pending = $client.BeginConnect('127.0.0.1', $Port, $null, $null)
    if (-not $pending.AsyncWaitHandle.WaitOne(700)) { return $false }
    $client.EndConnect($pending)
    return $true
  } catch { return $false } finally { $client.Dispose() }
}
function Save-ProcessRecord([string]$Name, $Process) {
  $record = @{ pid = $Process.Id; startedUtc = $Process.StartTime.ToUniversalTime().ToString('o') }
  [IO.File]::WriteAllText((Join-Path $runtime "$Name.pid.json"), ($record | ConvertTo-Json -Compress))
}
function Wait-Http([string]$Url, $Process, [int]$Seconds, [switch]$Json) {
  $until = (Get-Date).AddSeconds($Seconds)
  while ((Get-Date) -lt $until) {
    if ($Process -and $Process.HasExited) {
      $err = Join-Path $runtime 'backend.stderr.log'
      if ($Url -match ':3000') { $err = Join-Path $runtime 'frontend.stderr.log' }
      $tail = if (Test-Path $err) { (Get-Content $err -Tail 12) -join "`n" } else { 'no error log' }
      throw "Process $($Process.Id) exited during startup. $tail"
    }
    try {
      if ($Json) { return Invoke-RestMethod -Uri $Url -TimeoutSec 3 }
      $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 3
      if ($response.StatusCode -eq 200) { return $response }
    } catch { }
    Start-Sleep -Milliseconds 500
  }
  throw "Timed out waiting for $Url"
}

try {
  Write-Host 'Checking local Python and Node.js...'
  if (-not (Test-Path $python)) { throw "Python venv missing: $python. Create it and install backend/requirements.txt first." }
  if (-not $node) { throw 'Node.js not found in PATH. Install Node.js 20 or newer.' }
  if (-not (Test-Path (Join-Path $backend '.env'))) {
    Copy-Item (Join-Path $backend '.env.example') (Join-Path $backend '.env')
    Write-Host 'Created backend/.env from .env.example.'
  }
  & $python -c 'import fastapi, asyncpg, uvicorn, websockets, numpy' 2>$null
  if ($LASTEXITCODE -ne 0) { throw 'Backend dependencies are missing from .venv. Install backend/requirements.txt.' }

  Write-Host 'Checking PostgreSQL on localhost:5432...'
  if (-not (Test-TcpPort 5432)) {
    if ((Test-Path (Join-Path $pgData 'PG_VERSION')) -and (Test-Path $pgCtl)) {
      $pidFile = Join-Path $pgData 'postmaster.pid'
      if (Test-Path $pidFile) {
        $recordedPid = 0
        [void][int]::TryParse((Get-Content $pidFile -TotalCount 1), [ref]$recordedPid)
        $recordedProcess = if ($recordedPid -gt 0) { Get-Process -Id $recordedPid -ErrorAction SilentlyContinue } else { $null }
        $matchingPostgres = $recordedProcess -and $recordedProcess.ProcessName -eq 'postgres'
        if (-not (Test-TcpPort 5432) -and -not $matchingPostgres) {
          Remove-Item -LiteralPath $pidFile -Force
          Write-Warning 'Removed a stale PostgreSQL postmaster.pid after confirming no server was listening.'
        } elseif (-not (Test-TcpPort 5432)) {
          throw "PostgreSQL PID $recordedPid still exists but port 5432 is unavailable; refusing to remove its lock file."
        }
      }
      & $pgCtl status -D $pgData *> $null
      if ($LASTEXITCODE -ne 0) {
        $log = Join-Path $runtime 'postgres.log'
        & $pgCtl start -D $pgData -l $log -w
        if ($LASTEXITCODE -ne 0) { throw "Could not start the local PostgreSQL cluster; inspect $log for details." }
        [IO.File]::WriteAllText((Join-Path $runtime 'postgres.started'), $pgData)
        $started += 'PostgreSQL'
      }
    } else {
      $service = Get-Service -ErrorAction SilentlyContinue | Where-Object { $_.Name -match '^postgres' -or $_.DisplayName -match 'PostgreSQL' } | Select-Object -First 1
      if ($service) {
        try { Start-Service -Name $service.Name -ErrorAction Stop }
        catch { throw "PostgreSQL service '$($service.Name)' needs permission to start. Run START.cmd from an administrator session, or start PostgreSQL first. $($_.Exception.Message)" }
        [IO.File]::WriteAllText((Join-Path $runtime 'postgres.service.started'), $service.Name)
        $started += 'PostgreSQL service'
      } else {
        $docker = (Get-Command docker.exe -ErrorAction SilentlyContinue).Source
        if (-not $docker) { throw 'PostgreSQL is unreachable; no local cluster or service was found, and Docker Compose is unavailable.' }
        Push-Location $root
        try {
          $dockerOutput = & $docker compose up -d --wait postgres 2>&1
          if ($LASTEXITCODE -ne 0) { throw "Docker Compose could not start PostgreSQL. $($dockerOutput -join ' ')" }
        } finally { Pop-Location }
        [IO.File]::WriteAllText((Join-Path $runtime 'postgres.docker.started'), 'postgres')
        $started += 'PostgreSQL Docker'
      }
    }
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline -and -not (Test-TcpPort 5432)) { Start-Sleep -Milliseconds 500 }
    if (-not (Test-TcpPort 5432)) { throw 'PostgreSQL did not begin accepting connections on localhost:5432.' }
  }

  Write-Host 'PostgreSQL port is accepting connections.'
  $backendProc = $null
  if (Test-TcpPort 8000) {
    $health = Wait-Http 'http://127.0.0.1:8000/api/health' $null 5 -Json
    Write-Host 'Using the backend already listening on port 8000.'
  } else {
    Remove-Item (Join-Path $runtime 'backend.stop') -Force -ErrorAction SilentlyContinue
    $backendProc = Start-Process -FilePath $python -ArgumentList @('-m', 'app.worker') -WorkingDirectory $backend -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runtime 'backend.stdout.log') -RedirectStandardError (Join-Path $runtime 'backend.stderr.log')
    Save-ProcessRecord 'backend' $backendProc
    $started += 'backend'
    $health = Wait-Http 'http://127.0.0.1:8000/api/health' $backendProc 60 -Json
  }
  if (-not $health.database) { Write-Warning 'Backend API is up, but PostgreSQL is unavailable according to /api/health.' } else { Write-Host 'Backend API and database are ready.' }

  if (-not (Test-Path (Join-Path $frontend 'node_modules\next\dist\bin\next'))) {
    $npm = (Get-Command npm.cmd -ErrorAction SilentlyContinue).Source
    if (-not $npm) { throw 'npm.cmd not found. Install Node.js and npm.' }
    Write-Host 'Installing frontend dependencies declared in package.json...'
    Push-Location $frontend
    try { & $npm install --no-audit --no-fund; if ($LASTEXITCODE -ne 0) { throw 'npm install failed.' } } finally { Pop-Location }
  }

  Write-Host 'Checking Next.js frontend...'
  $frontendProc = $null
  if (Test-TcpPort 3000) {
    Wait-Http 'http://127.0.0.1:3000' $null 8 | Out-Null
    Write-Host 'Using the frontend already listening on port 3000.'
  } else {
    Remove-Item (Join-Path $runtime 'frontend.stop') -Force -ErrorAction SilentlyContinue
    $frontendProc = Start-Process -FilePath $node -ArgumentList @('server.cjs') -WorkingDirectory $frontend -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runtime 'frontend.stdout.log') -RedirectStandardError (Join-Path $runtime 'frontend.stderr.log')
    Save-ProcessRecord 'frontend' $frontendProc
    $started += 'frontend'
    Wait-Http 'http://127.0.0.1:3000' $frontendProc 90 | Out-Null
  }

  Write-Host ''
  Write-Host 'Binance Scanner is ready:'
  Write-Host '  UI:      http://127.0.0.1:3000'
  Write-Host '  API:     http://127.0.0.1:8000/docs'
  Write-Host "  Database: $(if ($health.database) { 'connected' } else { 'unavailable' })"
} catch {
  Write-Error $_
  foreach ($name in @('backend', 'frontend')) {
    if ($started -contains $name) {
      $marker = Join-Path $runtime "$name.stop"
      New-Item -ItemType File -Force -Path $marker | Out-Null
    }
  }
  Start-Sleep -Seconds 2
  if ($started -contains 'PostgreSQL') { & $pgCtl stop -D $pgData -m fast -w *> $null }
  exit 1
}