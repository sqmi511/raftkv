# Builds raftkv and starts a 3-node cluster plus the dashboard, each in its
# own PowerShell window, then opens the dashboard in your browser.
# Run from the repo root:  powershell -ExecutionPolicy Bypass -File scripts\local-cluster.ps1
# Stop everything by closing the windows (Ctrl+C in one window kills just that node).
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$root = (Get-Location).Path

go build -o bin\raftkv-server.exe ./cmd/server
go build -o bin\raftkv-cli.exe ./cmd/cli
go build -o bin\raftkv-dashboard.exe ./cmd/dashboard

$ports = 7001, 7002, 7003
foreach ($i in 0..2) {
    $self = $ports[$i]
    $peers = ($ports | Where-Object { $_ -ne $self } | ForEach-Object { "127.0.0.1:$_" }) -join ","
    $http = 8001 + $i
    $n = $i + 1
    $cmd = "`$Host.UI.RawUI.WindowTitle = 'raftkv node $n'; .\bin\raftkv-server.exe -listen 127.0.0.1:$self -peers $peers -http-addr 127.0.0.1:$http -data-dir data\n$n"
    Start-Process powershell -WorkingDirectory $root -ArgumentList "-NoExit", "-Command", $cmd
}

Start-Process powershell -WorkingDirectory $root -ArgumentList "-NoExit", "-Command", "`$Host.UI.RawUI.WindowTitle = 'raftkv dashboard'; .\bin\raftkv-dashboard.exe"
Start-Sleep -Seconds 2
Start-Process "http://127.0.0.1:9000"