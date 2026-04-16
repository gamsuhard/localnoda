param(
    [string]$InstanceId = "i-0b9c2749efd34e796",
    [string]$Region = "ap-southeast-1",
    [string]$Profile = "ai-agents-dev",
    [Int64]$SnapshotTotalBytes = 3134113120450,
    [double]$AssumedDownloadMiBPerSec = 16.5,
    [int]$IntervalSeconds = 900,
    [int]$MaxIterations = 0
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $root "provision\50_ssm_run_script.py"
$remoteScript = Join-Path $PSScriptRoot "10_remote_restore_status.sh"
$logDir = Join-Path $root "..\logs"
$logDir = [System.IO.Path]::GetFullPath($logDir)
$logPath = Join-Path $logDir "restore-watch.utf8.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-Log {
    param(
        [string]$Line
    )

    Write-Host $Line
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::AppendAllText($logPath, $Line + [Environment]::NewLine, $utf8)
}

function Format-Bytes {
    param(
        [double]$Bytes
    )

    if ($Bytes -ge 1TB) {
        return ("{0:N2} TiB" -f ($Bytes / 1TB))
    }
    if ($Bytes -ge 1GB) {
        return ("{0:N2} GiB" -f ($Bytes / 1GB))
    }
    if ($Bytes -ge 1MB) {
        return ("{0:N2} MiB" -f ($Bytes / 1MB))
    }
    return ("{0:N0} B" -f $Bytes)
}

$iteration = 0
$prevBytes = $null
$prevTime = $null
while ($true) {
    $iteration++
    $stamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
    $header = "===== $stamp iteration=$iteration instance=$InstanceId region=$Region ====="
    Write-Log $header

    try {
        $instanceRaw = & aws ec2 describe-instances `
            --profile $Profile `
            --region $Region `
            --instance-ids $InstanceId `
            --query "Reservations[0].Instances[0].State.Name" `
            --output text 2>&1
        $instanceState = ($instanceRaw | Select-Object -Last 1).ToString().Trim()
    } catch {
        $instanceState = "unknown"
        $msg = "ALERT instance_state_lookup_failed=$_"
        Write-Log $msg
    }

    try {
        $ssmRaw = & aws ssm describe-instance-information `
            --profile $Profile `
            --region $Region `
            --filters "Key=InstanceIds,Values=$InstanceId" `
            --query "InstanceInformationList[0].PingStatus" `
            --output text 2>&1
        $ssmStatus = ($ssmRaw | Select-Object -Last 1).ToString().Trim()
    } catch {
        $ssmStatus = "unknown"
        $msg = "ALERT ssm_status_lookup_failed=$_"
        Write-Log $msg
    }

    $precheck = "instance_state=$instanceState ssm_status=$ssmStatus"
    Write-Log $precheck

    if ($instanceState -ne "running" -or $ssmStatus -ne "Online") {
        $alert = "ALERT precheck_failed instance_state=$instanceState ssm_status=$ssmStatus"
        Write-Log $alert
    } else {
        try {
            $output = & python $runner `
                --instance-id $InstanceId `
                --region $Region `
                --profile $Profile `
                --comment "manual-restore-watch" `
                --timeout-seconds 180 `
                --script $remoteScript 2>&1

            foreach ($line in $output) {
                Write-Log $line
            }

            $bytesLine = $output | Where-Object { $_ -match '^output_dir_bytes=\d+$' } | Select-Object -Last 1
            $remoteTsLine = $output | Where-Object { $_ -match '^ts=' } | Select-Object -Last 1
            $restoreStartLine = $output | Where-Object { $_ -match '^\d{4}-\d{2}-\d{2}T.* restore_start$' } | Select-Object -Last 1
            if ($bytesLine) {
                $currentBytes = [int64](($bytesLine -split '=', 2)[1])
                if ($null -ne $prevBytes -and $null -ne $prevTime) {
                    $elapsedSeconds = [math]::Max(1.0, ((Get-Date) - $prevTime).TotalSeconds)
                    $deltaBytes = $currentBytes - $prevBytes
                    $deltaMbPerSec = [math]::Round(($deltaBytes / 1MB) / $elapsedSeconds, 2)
                    $growthLine = "growth_bytes=$deltaBytes growth_mib_per_sec=$deltaMbPerSec"
                    Write-Log $growthLine
                    if ($deltaBytes -le 0) {
                        $alert = "ALERT output_directory_not_growing"
                        Write-Log $alert
                    }
                }
                $prevBytes = $currentBytes
                $prevTime = Get-Date
            } else {
                $alert = "ALERT output_dir_bytes_missing"
                Write-Log $alert
            }

            if ($remoteTsLine -and $restoreStartLine) {
                $remoteTs = [datetimeoffset]::Parse(($remoteTsLine -split '=', 2)[1])
                $restoreStartUtc = [datetimeoffset]::Parse(($restoreStartLine -split ' ', 2)[0])
                $elapsedSinceRestoreSeconds = [math]::Max(0.0, ($remoteTs - $restoreStartUtc).TotalSeconds)
                $assumedDownloadBytesPerSecond = $AssumedDownloadMiBPerSec * 1MB
                $estimatedDownloadedBytes = [math]::Min([double]$SnapshotTotalBytes, $elapsedSinceRestoreSeconds * $assumedDownloadBytesPerSecond)
                $estimatedRemainingBytes = [math]::Max(0.0, [double]$SnapshotTotalBytes - $estimatedDownloadedBytes)
                if ($assumedDownloadBytesPerSecond -gt 0) {
                    $estimatedEtaSeconds = [int][math]::Ceiling($estimatedRemainingBytes / $assumedDownloadBytesPerSecond)
                } else {
                    $estimatedEtaSeconds = 0
                }

                Write-Log ("snapshot_total={0} ({1})" -f $SnapshotTotalBytes, (Format-Bytes $SnapshotTotalBytes))
                Write-Log ("estimated_downloaded={0:N0} ({1})" -f $estimatedDownloadedBytes, (Format-Bytes $estimatedDownloadedBytes))
                Write-Log ("estimated_remaining={0:N0} ({1})" -f $estimatedRemainingBytes, (Format-Bytes $estimatedRemainingBytes))
                Write-Log ("restore_elapsed_seconds={0}" -f $elapsedSinceRestoreSeconds)
                Write-Log ("assumed_download_mib_per_sec={0}" -f $AssumedDownloadMiBPerSec)
                Write-Log ("estimated_eta_seconds={0}" -f $estimatedEtaSeconds)
                Write-Log ("estimated_eta_hours={0:N2}" -f ($estimatedEtaSeconds / 3600.0))
            } else {
                $alert = "ALERT eta_inputs_missing"
                Write-Log $alert
            }

            if (-not ($output -match '^restore_process_alive=true$') -and -not ($output -match '^fullnode_service_active=active$') -and ($output -match '^nodeinfo_ready=false$')) {
                $alert = "ALERT restore_not_running_and_fullnode_not_ready"
                Write-Log $alert
            }
        } catch {
            $alert = "ALERT remote_status_failed=$_"
            Write-Log $alert
        }
    }

    if ($MaxIterations -gt 0 -and $iteration -ge $MaxIterations) {
        break
    }

    Start-Sleep -Seconds $IntervalSeconds
}
