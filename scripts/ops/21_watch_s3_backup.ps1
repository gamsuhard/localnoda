param(
    [string]$InstanceId = "i-0b9c2749efd34e796",
    [string]$Region = "ap-southeast-1",
    [string]$Profile = "ai-agents-dev",
    [string]$Bucket = "localnoda-tron-backup-apse1-20260417-775602",
    [string]$Prefix = "node-backup-20260417T1425Z/output-directory/",
    [Int64]$StopFullnodeThresholdBytes = 150GB,
    [int]$IntervalSeconds = 300,
    [int]$MaxIterations = 0
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $root "provision\50_ssm_run_script.py"
$remoteScript = Join-Path $PSScriptRoot "12_remote_s3_backup_status.sh"
$remoteStopScript = Join-Path $PSScriptRoot "14_stop_fullnode_low_disk.sh"
$logDir = Join-Path $root "..\logs"
$logDir = [System.IO.Path]::GetFullPath($logDir)
$logPath = Join-Path $logDir "s3-backup-watch.utf8.log"

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

function Get-S3Summary {
    param(
        [string]$BucketName,
        [string]$PrefixValue
    )

    $out = & aws s3 ls "s3://$BucketName/$PrefixValue" `
        --recursive `
        --summarize `
        --profile $Profile `
        --region $Region 2>&1

    $totalObjects = 0
    $totalSize = 0

    foreach ($line in ($out | Select-Object -Last 4)) {
        if ($line -match 'Total Objects:\s+(\d+)') {
            $totalObjects = [int64]$matches[1]
        }
        if ($line -match 'Total Size:\s+(\d+)') {
            $totalSize = [int64]$matches[1]
        }
    }

    return [pscustomobject]@{
        TotalObjects = $totalObjects
        TotalSize = $totalSize
    }
}

$iteration = 0
$prevUploadedBytes = $null
$prevTime = $null
while ($true) {
    $iteration++
    $stamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
    Write-Log "===== $stamp iteration=$iteration instance=$InstanceId bucket=$Bucket prefix=$Prefix ====="

    $instanceState = "unknown"
    $ssmStatus = "unknown"
    try {
        $instanceRaw = & aws ec2 describe-instances `
            --profile $Profile `
            --region $Region `
            --instance-ids $InstanceId `
            --query "Reservations[0].Instances[0].State.Name" `
            --output text 2>&1
        $instanceState = ($instanceRaw | Select-Object -Last 1).ToString().Trim()
    } catch {
        Write-Log "ALERT instance_state_lookup_failed=$_"
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
        Write-Log "ALERT ssm_status_lookup_failed=$_"
    }

    Write-Log "instance_state=$instanceState ssm_status=$ssmStatus"

    if ($instanceState -eq "running" -and $ssmStatus -eq "Online") {
        try {
            $remoteOutput = & python $runner `
                --instance-id $InstanceId `
                --region $Region `
                --profile $Profile `
                --comment "manual-s3-backup-watch" `
                --timeout-seconds 180 `
                --script $remoteScript 2>&1

            foreach ($line in $remoteOutput) {
                Write-Log $line
            }

            $localBytesLine = $remoteOutput | Where-Object { $_ -match '^local_total_bytes=\d+$' } | Select-Object -Last 1
            $elapsedLine = $remoteOutput | Where-Object { $_ -match '^backup_elapsed_seconds=\d+$' } | Select-Object -Last 1
            $aliveLine = $remoteOutput | Where-Object { $_ -match '^backup_process_alive=' } | Select-Object -Last 1
            $availBytesLine = $remoteOutput | Where-Object { $_ -match '^tron_data_avail_bytes=\d+$' } | Select-Object -Last 1
            $fullnodeActiveLine = $remoteOutput | Where-Object { $_ -match '^fullnode_service_active=' } | Select-Object -Last 1

            $s3Summary = Get-S3Summary -BucketName $Bucket -PrefixValue $Prefix
            $uploadedBytes = [double]$s3Summary.TotalSize
            $uploadedObjects = [int64]$s3Summary.TotalObjects

            Write-Log ("s3_uploaded_bytes={0} ({1})" -f [int64]$uploadedBytes, (Format-Bytes $uploadedBytes))
            Write-Log ("s3_uploaded_objects={0}" -f $uploadedObjects)

            if ($localBytesLine) {
                $localBytes = [double](($localBytesLine -split '=', 2)[1])
                $remainingBytes = [math]::Max(0.0, $localBytes - $uploadedBytes)
                $percent = if ($localBytes -gt 0) { [math]::Round(($uploadedBytes / $localBytes) * 100.0, 2) } else { 0.0 }

                Write-Log ("backup_percent={0}" -f $percent)
                Write-Log ("backup_remaining_bytes={0} ({1})" -f [int64]$remainingBytes, (Format-Bytes $remainingBytes))

                if ($elapsedLine) {
                    $elapsedSeconds = [double](($elapsedLine -split '=', 2)[1])
                    if ($elapsedSeconds -gt 0) {
                        $avgMiBPerSec = [math]::Round(($uploadedBytes / 1MB) / $elapsedSeconds, 2)
                        $avgEtaHours = if ($avgMiBPerSec -gt 0) { [math]::Round(($remainingBytes / 1MB) / $avgMiBPerSec / 3600.0, 2) } else { 0.0 }
                        Write-Log ("backup_avg_mib_per_sec={0}" -f $avgMiBPerSec)
                        Write-Log ("backup_avg_eta_hours={0}" -f $avgEtaHours)
                    }
                }

                if ($null -ne $prevUploadedBytes -and $null -ne $prevTime) {
                    $elapsedWindowSeconds = [math]::Max(1.0, ((Get-Date) - $prevTime).TotalSeconds)
                    $deltaBytes = $uploadedBytes - $prevUploadedBytes
                    $windowMiBPerSec = [math]::Round(($deltaBytes / 1MB) / $elapsedWindowSeconds, 2)
                    Write-Log ("backup_growth_bytes={0} backup_growth_mib_per_sec={1}" -f [int64]$deltaBytes, $windowMiBPerSec)

                    if ($windowMiBPerSec -gt 0) {
                        $windowEtaHours = [math]::Round(($remainingBytes / 1MB) / $windowMiBPerSec / 3600.0, 2)
                        Write-Log ("backup_window_eta_hours={0}" -f $windowEtaHours)
                    } else {
                        Write-Log "ALERT backup_not_growing"
                    }
                }

                $prevUploadedBytes = $uploadedBytes
                $prevTime = Get-Date
            } else {
                Write-Log "ALERT local_total_bytes_missing"
            }

            if ($availBytesLine) {
                $availBytes = [int64](($availBytesLine -split '=', 2)[1])
                Write-Log ("fullnode_stop_threshold_bytes={0} ({1})" -f $StopFullnodeThresholdBytes, (Format-Bytes $StopFullnodeThresholdBytes))
                Write-Log ("tron_data_avail_bytes={0} ({1})" -f $availBytes, (Format-Bytes $availBytes))

                if ($availBytes -le $StopFullnodeThresholdBytes -and $fullnodeActiveLine -eq 'fullnode_service_active=active') {
                    Write-Log "ALERT free_space_below_fullnode_stop_threshold"
                    try {
                        $stopOutput = & python $runner `
                            --instance-id $InstanceId `
                            --region $Region `
                            --profile $Profile `
                            --comment "manual-fullnode-stop-low-disk" `
                            --timeout-seconds 180 `
                            --script $remoteStopScript 2>&1
                        foreach ($line in $stopOutput) {
                            Write-Log $line
                        }
                    } catch {
                        Write-Log "ALERT fullnode_stop_failed=$_"
                    }
                }
            } else {
                Write-Log "ALERT tron_data_avail_bytes_missing"
            }

            if ($aliveLine -and $aliveLine -eq 'backup_process_alive=false') {
                Write-Log "ALERT backup_process_not_running"
            }
        } catch {
            Write-Log "ALERT backup_watch_failed=$_"
        }
    } else {
        Write-Log "ALERT precheck_failed instance_state=$instanceState ssm_status=$ssmStatus"
    }

    if ($MaxIterations -gt 0 -and $iteration -ge $MaxIterations) {
        break
    }

    Start-Sleep -Seconds $IntervalSeconds
}
