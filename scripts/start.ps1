# scripts/start.ps1 —— 全量替换，自动清理 7001 端口进程
# 运行方式（在项目根目录 wa-centralized-stack 下）：
#   powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1

$ErrorActionPreference = "Stop"

# 1. 切到项目根目录（scripts 的上一级目录）
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

# ===========================
# 2. 用 netstat + taskkill 清理占用 7001 的进程
# ===========================
$port = 7001
Write-Host "Checking process on port $port ..."

# 等价于：netstat -ano -p TCP | findstr ":7001"
$netstatOutput = & netstat -ano -p TCP 2>$null | Select-String ":$port"

if ($netstatOutput) {
    $procIds = @()

    foreach ($entry in $netstatOutput) {
        # 每一行类似：
        # TCP    0.0.0.0:7001   0.0.0.0:0   LISTENING   27312
        $line = $entry.Line
        if ($line -match "(\d+)\s*$") {
            # 这里用 $procId，避免和 PowerShell 内置 $PID 冲突
            $procId = [int]$matches[1]
            if (-not $procIds.Contains($procId)) {
                $procIds += $procId
            }
        }
    }

    foreach ($procId in $procIds) {
        Write-Host ("Killing PID {0} on port {1} ..." -f $procId, $port)
        # 等价于：taskkill /PID xxx /F /T
        & taskkill /PID $procId /F /T 2>$null | Out-Null
    }
} else {
    Write-Host "No process is using port $port."
}

# ===========================
# 3. 安装依赖
# ===========================
Write-Host "Installing common dependencies..."
try { npm --prefix "common" ci --silent } catch { npm --prefix "common" install --silent }

Write-Host "Installing bridge dependencies..."
try { npm --prefix "bridge" ci --silent } catch { npm --prefix "bridge" install --silent }

Write-Host "Installing collector dependencies..."
try { npm --prefix "collector" ci --silent } catch { npm --prefix "collector" install --silent }

Write-Host "Installing sender dependencies..."
try { npm --prefix "sender" ci --silent } catch { npm --prefix "sender" install --silent }

# ===========================
# 4. 启动后台服务（collector / sender）
# ===========================
Write-Host "Launching collector (background)..."
Start-Process -FilePath "cmd.exe" -ArgumentList '/c node .\collector\src\index.js > .\collector\collector.log 2>&1' -WindowStyle Hidden

Write-Host "Launching sender webhook (background)..."
Start-Process -FilePath "cmd.exe" -ArgumentList '/c node .\sender\src\webhook.js > .\sender\webhook.log 2>&1' -WindowStyle Hidden

Write-Host "Launching sender worker (background)..."
Start-Process -FilePath "cmd.exe" -ArgumentList '/c node .\sender\src\worker.js  > .\sender\worker.log  2>&1' -WindowStyle Hidden

Start-Sleep -Seconds 2

# ===========================
# 5. 前台启动 whatsapp-manager
# ===========================
Write-Host "Launching whatsapp-manager (foreground)..."
node .\bridge\whatsapp-manager.js
