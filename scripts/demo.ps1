<#
.SYNOPSIS
  Live demo of raftkv: start a 3-node cluster, write data, kill the leader,
  keep serving, lose the majority, and recover. Everything runs as hidden
  background processes; logs go to data\demo.

.PARAMETER Dashboard
  Also start the web dashboard and open it in the browser (great for the GIF).
.PARAMETER KeepRunning
  Leave the cluster running when the demo ends.
.PARAMETER PauseSeconds
  Pause between steps, so a viewer can read the output.

  Run from the repo root:  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 -Dashboard
  WARNING: it stops every running raftkv-server and raftkv-dashboard process first.
#>
param(
    [switch]$Dashboard,
    [switch]$KeepRunning,
    [int]$PauseSeconds = 2
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$root = (Get-Location).Path

$server = Join-Path $root "bin\raftkv-server.exe"
$cli = Join-Path $root "bin\raftkv-cli.exe"
$dashExe = Join-Path $root "bin\raftkv-dashboard.exe"
$demoDir = Join-Path $root "data\demo"
$addrs = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"
$procs = @{}
$dash = $null

function Step([string]$text) {
    Write-Host ""
    Write-Host "==> $text" -ForegroundColor Cyan
    Start-Sleep -Seconds $PauseSeconds
}
function Note([string]$text) { Write-Host "    $text" -ForegroundColor DarkGray }

function Rkv { & $cli -addr $addrs @args }

function Get-NodeStatus([int]$i) {
    try { Invoke-RestMethod "http://127.0.0.1:$(8000 + $i)/status" -TimeoutSec 1 } catch { $null }
}

function Find-Leader {
    foreach ($i in 1..3) {
        if ($procs[$i] -and -not $procs[$i].HasExited) {
            $s = Get-NodeStatus $i
            if ($s -and $s.state -eq "leader") { return $i }
        }
    }
    return 0
}

function Wait-Leader([int]$timeoutSec = 20) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $l = Find-Leader
        if ($l -ne 0) { return $l }
        Start-Sleep -Milliseconds 100
    }
    throw "no leader after $timeoutSec seconds (see $demoDir\*.log)"
}

function Start-Node([int]$i) {
    $peers = (1..3 | Where-Object { $_ -ne $i } | ForEach-Object { "127.0.0.1:$(7000 + $_)" }) -join ","
    $argList = @("-listen", "127.0.0.1:$(7000 + $i)", "-peers", $peers,
                 "-http-addr", "127.0.0.1:$(8000 + $i)", "-data-dir", (Join-Path $demoDir "n$i"))
    for ($try = 1; $try -le 3; $try++) {
        $procs[$i] = Start-Process -FilePath $server -ArgumentList $argList -PassThru -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $demoDir "n$i.out.log") `
            -RedirectStandardError (Join-Path $demoDir "n$i.log")
        Start-Sleep -Milliseconds 700
        if (-not $procs[$i].HasExited) { return }   # a port that is not free yet makes it exit at once
    }
    throw "node $i would not start (see $demoDir\n$i.log)"
}

function Kill-Node([int]$i) {
    Stop-Process -Id $procs[$i].Id -Force
    $procs[$i].WaitForExit(5000) | Out-Null
}

function Show-Cluster {
    foreach ($i in 1..3) {
        $s = if ($procs[$i] -and -not $procs[$i].HasExited) { Get-NodeStatus $i } else { $null }
        if ($s) {
            "{0}  {1,-9} term={2,-4} commit={3}/{4}" -f $s.id, $s.state, $s.term, $s.commit_index, $s.last_index | Write-Host
        } else {
            "127.0.0.1:$(7000 + $i)  DOWN" | Write-Host -ForegroundColor Red
        }
    }
}

function Wait-CaughtUp([int]$node, [int]$timeoutSec = 20) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $leader = Find-Leader
        $a = Get-NodeStatus $node
        $b = if ($leader -ne 0) { Get-NodeStatus $leader } else { $null }
        if ($a -and $b -and $a.commit_index -ge $b.commit_index -and $a.applied_index -ge $b.commit_index) { return }
        Start-Sleep -Milliseconds 200
    }
    throw "node $node did not catch up"
}

try {
    Step "Building raftkv"
    go build -o bin\raftkv-server.exe ./cmd/server
    go build -o bin\raftkv-cli.exe ./cmd/cli
    go build -o bin\raftkv-dashboard.exe ./cmd/dashboard

    Note "Stopping leftover raftkv processes and wiping old demo data"
    Get-Process raftkv-server, raftkv-dashboard -ErrorAction SilentlyContinue | Stop-Process -Force
    Start-Sleep -Seconds 1
    Remove-Item -Recurse -Force $demoDir -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force $demoDir | Out-Null

    Step "Starting a 3-node cluster"
    foreach ($i in 1..3) { Start-Node $i }
    if ($Dashboard) {
        $dash = Start-Process -FilePath $dashExe -PassThru -WindowStyle Hidden
        Start-Sleep -Seconds 1
        Start-Process "http://127.0.0.1:9000"
        Note "Dashboard: http://127.0.0.1:9000"
    }
    Wait-Leader | Out-Null
    Show-Cluster

    Step "Writing 20 keys through the client (it finds the leader by itself)"
    foreach ($n in 1..20) {
        Rkv set "user:$n" "value-$n" | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "write $n failed" }
    }
    Write-Host "20 writes committed on a majority of nodes"
    Write-Host -NoNewline "get user:7 -> "
    Rkv get user:7

    $leader = Find-Leader
    Step "Killing the LEADER (node $leader)"
    $sw = [Diagnostics.Stopwatch]::StartNew()
    Kill-Node $leader
    Rkv set after-failover "still-works" | Out-Null
    $sw.Stop()
    if ($LASTEXITCODE -ne 0) { throw "the write after the failover failed" }
    Write-Host ("A write succeeded {0:N0} ms after the leader died (new election + client retry)" -f $sw.Elapsed.TotalMilliseconds) -ForegroundColor Green
    $newLeader = Wait-Leader
    Write-Host "New leader: node $newLeader"
    Show-Cluster

    Step "Checking that no acknowledged write was lost"
    $ok = 0
    foreach ($n in 1..20) {
        if ((Rkv get "user:$n") -eq "value-$n") { $ok++ }
    }
    Write-Host "$ok / 20 keys read back correctly from the surviving majority" -ForegroundColor Green

    Step "Restarting the killed node: it rejoins and catches up from its own disk"
    Start-Node $leader
    Wait-CaughtUp $leader
    Show-Cluster

    Step "Now killing TWO nodes: the cluster loses its majority"
    $current = Wait-Leader
    $victim = (1..3 | Where-Object { $_ -ne $current } | Select-Object -First 1)
    Kill-Node $current
    Kill-Node $victim
    Show-Cluster
    Write-Host "Trying to write with only 1 of 3 nodes alive (3 s timeout)..."
    Rkv -timeout 3s set should-fail "no-majority"
    Write-Host "Refused (exit code $LASTEXITCODE). Raft chooses consistency over availability." -ForegroundColor Yellow

    Step "Restarting both nodes: the majority returns and writes work again"
    Start-Node $current
    Start-Node $victim
    Wait-Leader | Out-Null
    Rkv set recovered "yes" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "the write after recovery failed" }
    Write-Host -NoNewline "get after-failover -> "
    Rkv get after-failover
    Show-Cluster

    Write-Host ""
    Write-Host "Demo complete." -ForegroundColor Green
}
finally {
    if (-not $KeepRunning) {
        foreach ($p in $procs.Values) {
            if ($p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
        }
        if ($dash -and -not $dash.HasExited) { Stop-Process -Id $dash.Id -Force -ErrorAction SilentlyContinue }
    } else {
        Note "Cluster left running (logs in $demoDir). Stop it with: Get-Process raftkv-* | Stop-Process"
    }
}
