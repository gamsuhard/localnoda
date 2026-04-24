param(
    [string]$InstanceId = "i-0a310a59ba9ee4849",
    [string]$Region = "eu-central-1",
    [string]$Profile = "DR",
    [string]$SshUser = "ec2-user",
    [string]$SshKeyPath = "D:\Users\codex\.ssh\goldusdt-v2-dr-dump-key3.pem",
    [string]$SecretsPath = "",
    [int]$SsmTimeoutSeconds = 120,
    [int]$IntervalSeconds = 900,
    [int]$MaxIterations = 0
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $root "..\logs"
$logDir = [System.IO.Path]::GetFullPath($logDir)
$logPath = Join-Path $logDir "dr-loader-watch.utf8.log"
$pidPath = Join-Path $logDir "dr-loader-watch.pid"
$finalReportPath = Join-Path $logDir "dr-loader-final-report.md"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

if ([string]::IsNullOrWhiteSpace($SecretsPath)) {
    if (-not [string]::IsNullOrWhiteSpace($env:CODEX_SHARED_SECRETS_FILE)) {
        $SecretsPath = $env:CODEX_SHARED_SECRETS_FILE
    } else {
        $candidateSecretsPaths = @(
            "D:\Users\codex\.codex\shared-secrets\master-secrets.json",
            (Join-Path $HOME ".codex\shared-secrets\master-secrets.json")
        )
        foreach ($candidatePath in $candidateSecretsPaths) {
            if (Test-Path -LiteralPath $candidatePath) {
                $SecretsPath = $candidatePath
                break
            }
        }
        if ([string]::IsNullOrWhiteSpace($SecretsPath)) {
            $SecretsPath = "D:\Users\codex\.codex\shared-secrets\master-secrets.json"
        }
    }
}

[System.IO.File]::WriteAllText($pidPath, $PID.ToString(), (New-Object System.Text.UTF8Encoding($false)))
$utf8 = New-Object System.Text.UTF8Encoding($false)

$streams = @{
    trx = @{
        Label = "TRX"
        RunId = "tron-trx-backfill-20240901-20241216-20260420t190707z"
        Database = "tron_trx_inbound_20240901_20241216_20260420t190707z"
        Table = "trx_inbound_transfer_events"
        AuditTable = "load_audit"
        DbPath = "/srv/local-tron-usdt-backfill/runtime/loader_state_trx_inbound.sqlite"
        TotalSegments = 425
        ServiceName = "local-tron-trx-incremental-loader@1.service"
        TimerName = "local-tron-trx-incremental-loader@1.timer"
        AliasPrefix = "s3://goldusdt-v2-stage-backup-dr-euc1-192802165401-20260422/providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/tron-trx-backfill-20240901-20241216-20260420t190707z/"
        NestedPrefix = "s3://goldusdt-v2-stage-backup-dr-euc1-192802165401-20260422/goldusdt-v2-stage-913378704801-raw/providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/tron-trx-backfill-20240901-20241216-20260420t190707z/"
    }
    res = @{
        Label = "RESOURCE"
        RunId = "tron-resource-delegation-backfill-20240901-20241216-20260420t190707z"
        Database = "tron_resource_delegation_20240901_20241216_20260420t190707z"
        Table = "resource_delegation_observations"
        AuditTable = "load_audit"
        DbPath = "/srv/local-tron-usdt-backfill/runtime/loader_state_resource_delegation.sqlite"
        TotalSegments = 276
        ServiceName = "local-tron-resource-delegation-incremental-loader@1.service"
        TimerName = "local-tron-resource-delegation-incremental-loader@1.timer"
        AliasPrefix = "s3://goldusdt-v2-stage-backup-dr-euc1-192802165401-20260422/providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/tron-resource-delegation-backfill-20240901-20241216-20260420t190707z/"
        NestedPrefix = "s3://goldusdt-v2-stage-backup-dr-euc1-192802165401-20260422/goldusdt-v2-stage-913378704801-raw/providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/tron-resource-delegation-backfill-20240901-20241216-20260420t190707z/"
    }
}

function Write-Log {
    param(
        [string]$Line
    )

    $stamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
    $fullLine = "$stamp $Line"
    Write-Host $fullLine
    [System.IO.File]::AppendAllText($logPath, $fullLine + [Environment]::NewLine, $utf8)
}

function Load-Secrets {
    $raw = Get-Content -Raw -Path $SecretsPath
    return ($raw | ConvertFrom-Json)
}

function Get-CountValue {
    param(
        [object]$CountsObject,
        [string]$StatusName
    )

    if ($null -eq $CountsObject) {
        return 0
    }

    $prop = $CountsObject.PSObject.Properties[$StatusName]
    if ($null -eq $prop) {
        return 0
    }
    return [int]$prop.Value
}

function Format-FloatOrNa {
    param(
        [object]$Value
    )

    if ($null -eq $Value -or $Value -eq "") {
        return "n/a"
    }
    return ("{0:N2}" -f ([double]$Value))
}

function Invoke-ClickHouseProbe {
    $script = @'
import base64
import json
import sys
import urllib.request

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    secrets = json.load(f)
ch = secrets["manual_credentials"]["clickhouse"]["DR"]
endpoint = f"https://{ch['https_endpoint']}/?database={ch['sql_database']}"
auth = base64.b64encode(f"{ch['sql_user']}:{ch['sql_password']}".encode()).decode()
queries = {
    "trx": "SELECT count() AS rows, min(block_number) AS min_block, max(block_number) AS max_block FROM tron_trx_inbound_20240901_20241216_20260420t190707z.trx_inbound_transfer_events FORMAT JSONEachRow",
    "trx_audit": "SELECT count() AS batches, sum(inserted_row_count) AS inserted_rows, max(segment_id) AS last_segment_id FROM tron_trx_inbound_20240901_20241216_20260420t190707z.load_audit FORMAT JSONEachRow",
    "res": "SELECT count() AS rows, min(block_number) AS min_block, max(block_number) AS max_block FROM tron_resource_delegation_20240901_20241216_20260420t190707z.resource_delegation_observations FORMAT JSONEachRow",
    "res_audit": "SELECT count() AS batches, sum(inserted_row_count) AS inserted_rows, max(segment_id) AS last_segment_id FROM tron_resource_delegation_20240901_20241216_20260420t190707z.load_audit FORMAT JSONEachRow",
    "res_types": "SELECT resource_type, contract_type, count() AS rows FROM tron_resource_delegation_20240901_20241216_20260420t190707z.resource_delegation_observations GROUP BY resource_type, contract_type ORDER BY rows DESC FORMAT JSONEachRow",
}
result = {}
for name, query in queries.items():
    req = urllib.request.Request(
        endpoint,
        data=query.encode("utf-8"),
        headers={"Authorization": f"Basic {auth}"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        text = resp.read().decode("utf-8").strip()
    if not text:
        result[name] = None
        continue
    if "\n" in text:
        result[name] = [json.loads(line) for line in text.splitlines() if line.strip()]
    else:
        result[name] = json.loads(text)
print(json.dumps(result))
'@

    $tempPath = Join-Path $env:TEMP ("codex-ch-probe-" + [guid]::NewGuid().ToString() + ".py")
    [System.IO.File]::WriteAllText($tempPath, $script, (New-Object System.Text.UTF8Encoding($false)))
    try {
        $json = & python $tempPath $SecretsPath
        return ($json | ConvertFrom-Json)
    } finally {
        Remove-Item -Force -Path $tempPath -ErrorAction SilentlyContinue
    }
}

function Get-InstanceState {
    $query = "Reservations[0].Instances[0].{State:State.Name,PublicIp:PublicIpAddress,PrivateIp:PrivateIpAddress}"
    $json = aws --profile $Profile --region $Region ec2 describe-instances --instance-ids $InstanceId --query $query --output json
    return ($json | ConvertFrom-Json)
}

function Get-LatestCloudWatchValue {
    param(
        [string]$MetricName,
        [string[]]$Stats,
        [datetime]$StartTime
    )

    $args = @(
        "--profile", $Profile,
        "--region", $Region,
        "cloudwatch", "get-metric-statistics",
        "--namespace", "AWS/EC2",
        "--metric-name", $MetricName,
        "--dimensions", "Name=InstanceId,Value=$InstanceId",
        "--start-time", $StartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"),
        "--end-time", (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"),
        "--period", "300",
        "--output", "json"
    )
    foreach ($stat in $Stats) {
        $args += @("--statistics", $stat)
    }

    $json = & aws @args
    $data = $json | ConvertFrom-Json
    if (-not $data.Datapoints -or $data.Datapoints.Count -eq 0) {
        return $null
    }
    return ($data.Datapoints | Sort-Object Timestamp | Select-Object -Last 1)
}

function Invoke-RemoteBash {
    param(
        [string]$RemoteHost,
        [string]$ScriptText
    )

    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($ScriptText))
    $remoteCommand = "printf '%s' '$encoded' | base64 -d | bash"
    $parameters = @{
        commands = @($remoteCommand)
        executionTimeout = @([string]$SsmTimeoutSeconds)
    } | ConvertTo-Json -Compress
    $parametersPath = Join-Path $env:TEMP ("codex-ssm-params-" + [guid]::NewGuid().ToString() + ".json")
    [System.IO.File]::WriteAllText($parametersPath, $parameters, (New-Object System.Text.UTF8Encoding($false)))
    try {
        $commandId = aws --profile $Profile --region $Region ssm send-command `
            --instance-ids $InstanceId `
            --document-name "AWS-RunShellScript" `
            --comment "dr-loader-watch remote bash" `
            --parameters "file://$parametersPath" `
            --query "Command.CommandId" `
            --output text
        if ([string]::IsNullOrWhiteSpace($commandId)) {
            throw "ssm send-command returned empty command id"
        }

        aws --profile $Profile --region $Region ssm wait command-executed `
            --command-id $commandId `
            --instance-id $InstanceId

        $invocationJson = aws --profile $Profile --region $Region ssm get-command-invocation `
            --command-id $commandId `
            --instance-id $InstanceId `
            --query "{Status:Status,ResponseCode:ResponseCode,Stdout:StandardOutputContent,Stderr:StandardErrorContent}" `
            --output json
    } finally {
        Remove-Item -Force -Path $parametersPath -ErrorAction SilentlyContinue
    }
    $invocation = $invocationJson | ConvertFrom-Json
    if ($invocation.Status -ne "Success" -or [int]$invocation.ResponseCode -ne 0) {
        throw ("ssm command failed status={0} response_code={1} stdout={2} stderr={3}" -f $invocation.Status, $invocation.ResponseCode, $invocation.Stdout, $invocation.Stderr)
    }

    if ([string]::IsNullOrWhiteSpace($invocation.Stdout)) {
        return @()
    }
    return @($invocation.Stdout -split "`r`n|`n|`r" | Where-Object { $_ -ne "" })
}

function Get-RemoteStatus {
    param(
        [string]$RemoteHost
    )

    $script = @'
set -e
trx_service="local-tron-trx-incremental-loader@1.service"
trx_timer="local-tron-trx-incremental-loader@1.timer"
res_service="local-tron-resource-delegation-incremental-loader@1.service"
res_timer="local-tron-resource-delegation-incremental-loader@1.timer"

echo "hostname=$(hostname)"
echo "uptime=$(uptime -p)"
echo "trx_service_active=$(systemctl show "$trx_service" --property ActiveState --value)"
echo "trx_service_substate=$(systemctl show "$trx_service" --property SubState --value)"
echo "trx_service_result=$(systemctl show "$trx_service" --property Result --value)"
echo "trx_timer_active=$(systemctl show "$trx_timer" --property ActiveState --value)"
echo "trx_timer_unit_state=$(systemctl show "$trx_timer" --property UnitFileState --value)"
echo "res_service_active=$(systemctl show "$res_service" --property ActiveState --value)"
echo "res_service_substate=$(systemctl show "$res_service" --property SubState --value)"
echo "res_service_result=$(systemctl show "$res_service" --property Result --value)"
echo "res_timer_active=$(systemctl show "$res_timer" --property ActiveState --value)"
echo "res_timer_unit_state=$(systemctl show "$res_timer" --property UnitFileState --value)"
echo "trx_pid=$(pgrep -f 'run-id tron-trx-backfill-20240901-20241216-20260420t190707z' | head -n1 || true)"
echo "res_pid=$(pgrep -f 'run-id tron-resource-delegation-backfill-20240901-20241216-20260420t190707z' | head -n1 || true)"
free -m | awk '/Mem:/ {print "mem_total_mb="$2"\nmem_used_mb="$3"\nmem_free_mb="$4"\nmem_available_mb="$7} /Swap:/ {print "swap_total_mb="$2"\nswap_used_mb="$3"\nswap_free_mb="$4}'
'@

    $lines = Invoke-RemoteBash -RemoteHost $RemoteHost -ScriptText $script
    $map = @{}
    foreach ($line in $lines) {
        if ($line -match '^[A-Za-z0-9_]+=(.*)$') {
            $name, $value = $line -split '=', 2
            $map[$name] = $value
        }
    }
    return $map
}

function Ensure-RemoteStream {
    param(
        [string]$RemoteHost,
        [string]$ServiceName,
        [string]$TimerName
    )

    $script = @"
set -e
sudo systemctl enable --now $TimerName
sudo systemctl reset-failed $ServiceName || true
sudo systemctl start $ServiceName
systemctl show $ServiceName --property ActiveState --property SubState --property Result --no-pager
"@

    return (Invoke-RemoteBash -RemoteHost $RemoteHost -ScriptText $script)
}

function Get-RemoteLoaderState {
    param(
        [string]$RemoteHost
    )

    $script = @'
python3 - <<'PY'
import json
import sqlite3

streams = {
    "trx": {
        "run_id": "tron-trx-backfill-20240901-20241216-20260420t190707z",
        "db_path": "/srv/local-tron-usdt-backfill/runtime/loader_state_trx_inbound.sqlite",
    },
    "res": {
        "run_id": "tron-resource-delegation-backfill-20240901-20241216-20260420t190707z",
        "db_path": "/srv/local-tron-usdt-backfill/runtime/loader_state_resource_delegation.sqlite",
    },
}

result = {}
for name, cfg in streams.items():
    payload = {
        "run_id": cfg["run_id"],
        "counts": {},
        "latest_failed_segment": "",
        "latest_failed_error": "",
        "latest_validated_segment": "",
    }
    conn = sqlite3.connect(cfg["db_path"])
    cur = conn.cursor()
    payload["counts"] = {
        status: count
        for status, count in cur.execute(
            "SELECT status, count(1) FROM loaded_segments WHERE run_id = ? GROUP BY status",
            (cfg["run_id"],),
        ).fetchall()
    }
    failed = cur.execute(
        "SELECT segment_id, last_error FROM loaded_segments WHERE run_id = ? AND status IN ('failed','quarantined') ORDER BY load_finished_at DESC, claimed_at DESC LIMIT 1",
        (cfg["run_id"],),
    ).fetchone()
    if failed:
        payload["latest_failed_segment"] = failed[0] or ""
        payload["latest_failed_error"] = ((failed[1] or "").splitlines() or [""])[0]
    validated = cur.execute(
        "SELECT segment_id FROM loaded_segments WHERE run_id = ? AND status IN ('validated','skipped') ORDER BY load_finished_at DESC, segment_id DESC LIMIT 1",
        (cfg["run_id"],),
    ).fetchone()
    if validated:
        payload["latest_validated_segment"] = validated[0] or ""
    conn.close()
    result[name] = payload

print(json.dumps(result))
PY
'@

    $json = Invoke-RemoteBash -RemoteHost $RemoteHost -ScriptText $script
    $text = (($json | Where-Object { $_ -match '^\{' }) -join "")
    if ([string]::IsNullOrWhiteSpace($text)) {
        throw "remote loader state returned empty payload"
    }
    return ($text | ConvertFrom-Json)
}

function Requeue-RemoteErrors {
    param(
        [string]$RemoteHost,
        [hashtable]$StreamConfig
    )

    $script = @"
python3 - <<'PY'
import json
import sqlite3

run_id = "$($StreamConfig.RunId)"
db_path = "$($StreamConfig.DbPath)"
conn = sqlite3.connect(db_path)
cur = conn.cursor()
before = {
    status: count
    for status, count in cur.execute(
        "SELECT status, count(1) FROM loaded_segments WHERE run_id = ? GROUP BY status",
        (run_id,),
    ).fetchall()
}
cur.execute(
    \"\"\"
    UPDATE loaded_segments
    SET status = 'pending',
        claim_token = NULL,
        attempts = 0,
        bytes_read = 0,
        record_count = 0,
        event_rows = 0,
        leg_rows = 0,
        s3_read_ms = 0,
        normalize_ms = 0,
        stage_ms = 0,
        merge_ms = 0,
        audit_ms = 0,
        validation_ms = 0,
        claimed_at = NULL,
        load_started_at = NULL,
        merged_at = NULL,
        load_finished_at = NULL
    WHERE run_id = ?
      AND status IN ('failed', 'quarantined')
    \"\"\",
    (run_id,),
)
affected = cur.rowcount
conn.commit()
after = {
    status: count
    for status, count in cur.execute(
        "SELECT status, count(1) FROM loaded_segments WHERE run_id = ? GROUP BY status",
        (run_id,),
    ).fetchall()
}
conn.close()
print(json.dumps({"affected": affected, "before": before, "after": after}))
PY
"@

    $result = Invoke-RemoteBash -RemoteHost $RemoteHost -ScriptText $script
    $text = (($result | Where-Object { $_ -match '^\{' }) -join "")
    if ([string]::IsNullOrWhiteSpace($text)) {
        throw "remote requeue returned empty payload"
    }
    return ($text | ConvertFrom-Json)
}

function Write-FinalReport {
    param(
        [object]$Probe,
        [object]$LoaderState
    )

    $generatedAt = Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK'
    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add("# DR loader final report")
    $lines.Add("")
    $lines.Add('- Generated at: `' + $generatedAt + '`')
    $lines.Add('- Instance id: `' + $InstanceId + '`')
    $lines.Add('- Region: `' + $Region + '`')
    $lines.Add("")
    $lines.Add("## Final validation")
    foreach ($streamName in @("trx", "res")) {
        $cfg = $streams[$streamName]
        $stateObj = $LoaderState.$streamName
        $probeObj = $Probe.$streamName
        $auditObj = $Probe.("{0}_audit" -f $streamName)
        $validated = Get-CountValue -CountsObject $stateObj.counts -StatusName "validated"
        $skipped = Get-CountValue -CountsObject $stateObj.counts -StatusName "skipped"
        $failed = Get-CountValue -CountsObject $stateObj.counts -StatusName "failed"
        $quarantined = Get-CountValue -CountsObject $stateObj.counts -StatusName "quarantined"
        $pending = Get-CountValue -CountsObject $stateObj.counts -StatusName "pending"
        $loading = Get-CountValue -CountsObject $stateObj.counts -StatusName "loading"
        $claimed = Get-CountValue -CountsObject $stateObj.counts -StatusName "claimed"
        $merged = Get-CountValue -CountsObject $stateObj.counts -StatusName "merged"
        $minBlock = if ($probeObj.min_block) { $probeObj.min_block } else { 0 }
        $maxBlock = if ($probeObj.max_block) { $probeObj.max_block } else { 0 }
        $lines.Add("### $($cfg.Label)")
        $lines.Add('- segments: `' + "validated=$validated skipped=$skipped pending=$pending claimed=$claimed loading=$loading merged=$merged failed=$failed quarantined=$quarantined total=$($cfg.TotalSegments)" + '`')
        $lines.Add('- clickhouse rows: `' + $probeObj.rows + '`')
        $lines.Add('- block frontier: `' + $minBlock + '->' + $maxBlock + '`')
        $lines.Add('- audit batches: `' + $auditObj.batches + '` inserted rows: `' + $auditObj.inserted_rows + '` last segment: `' + $auditObj.last_segment_id + '`')
        $lines.Add("")
    }
    $lines.Add("## S3 cleanup guidance")
    $lines.Add("Delete first:")
    $lines.Add("- temporary alias copy prefixes used only to satisfy manifest paths")
    foreach ($streamName in @("trx", "res")) {
        $lines.Add('  - `' + $streams[$streamName].AliasPrefix + '`')
    }
    $lines.Add("")
    $lines.Add("Delete optionally after you no longer need replay/rebuild from raw:")
    $lines.Add("- canonical nested raw prefixes for these two runs")
    foreach ($streamName in @("trx", "res")) {
        $lines.Add('  - `' + $streams[$streamName].NestedPrefix + '`')
    }
    $lines.Add("")
    $lines.Add("Keep:")
    $lines.Add("- unrelated DR backup bucket contents")
    $lines.Add("- full Singapore/fullnode backups unless you explicitly decide to remove them")
    if ($Probe.res_types) {
        $lines.Add("")
        $lines.Add("## Resource delegation note")
        $lines.Add('- current raw source does not carry `ENERGY/BANDWIDTH`; `resource_type` materializes as observed in ClickHouse below')
        foreach ($row in $Probe.res_types) {
            $lines.Add('  - `' + $row.contract_type + '` / `' + $row.resource_type + '` => `' + $row.rows + '` rows')
        }
    }

    [System.IO.File]::WriteAllLines($finalReportPath, $lines, $utf8)
}

function Stop-TargetInstance {
    Write-Log "FINAL stop_instance_requested"
    $stopJson = aws --profile $Profile --region $Region ec2 stop-instances --instance-ids $InstanceId --output json
    Write-Log ("stop_instances_response=" + ($stopJson -join " "))
    aws --profile $Profile --region $Region ec2 wait instance-stopped --instance-ids $InstanceId
    Write-Log "FINAL instance_stopped"
}

$secrets = Load-Secrets

$state = @{
    trx = @{
        PrevDoneSegments = $null
        PrevRows = $null
        PrevTime = $null
        ReconcilePasses = 0
        Completed = $false
    }
    res = @{
        PrevDoneSegments = $null
        PrevRows = $null
        PrevTime = $null
        ReconcilePasses = 0
        Completed = $false
    }
}

$maxReconcilePasses = 3
$iteration = 0

while ($true) {
    $iteration++
    Write-Log "===== iteration=$iteration instance=$InstanceId region=$Region ====="

    try {
        $instance = Get-InstanceState
    } catch {
        Write-Log "ALERT instance_lookup_failed=$_"
        if ($MaxIterations -gt 0 -and $iteration -ge $MaxIterations) { break }
        Start-Sleep -Seconds $IntervalSeconds
        continue
    }

    Write-Log ("instance_state={0} public_ip={1} private_ip={2}" -f $instance.State, $instance.PublicIp, $instance.PrivateIp)
    if ($instance.State -ne "running") {
        Write-Log "ALERT instance_not_running"
        break
    }

    try {
        $remote = Get-RemoteStatus -RemoteHost $instance.PublicIp
        $loaderState = Get-RemoteLoaderState -RemoteHost $instance.PublicIp
    } catch {
        Write-Log "ALERT remote_probe_failed=$_"
        if ($MaxIterations -gt 0 -and $iteration -ge $MaxIterations) { break }
        Start-Sleep -Seconds $IntervalSeconds
        continue
    }

    Write-Log ("host={0} uptime={1}" -f $remote.hostname, $remote.uptime)
    Write-Log ("mem_available_mb={0} swap_used_mb={1}" -f $remote.mem_available_mb, $remote.swap_used_mb)
    Write-Log ("trx_service={0}/{1}/{2} trx_timer={3}/{4} trx_pid={5}" -f $remote.trx_service_active, $remote.trx_service_substate, $remote.trx_service_result, $remote.trx_timer_active, $remote.trx_timer_unit_state, $remote.trx_pid)
    Write-Log ("res_service={0}/{1}/{2} res_timer={3}/{4} res_pid={5}" -f $remote.res_service_active, $remote.res_service_substate, $remote.res_service_result, $remote.res_timer_active, $remote.res_timer_unit_state, $remote.res_pid)

    foreach ($streamName in @("trx", "res")) {
        $cfg = $streams[$streamName]
        $remoteState = $loaderState.$streamName
        $counts = $remoteState.counts
        $validated = Get-CountValue -CountsObject $counts -StatusName "validated"
        $skipped = Get-CountValue -CountsObject $counts -StatusName "skipped"
        $pending = Get-CountValue -CountsObject $counts -StatusName "pending"
        $claimed = Get-CountValue -CountsObject $counts -StatusName "claimed"
        $loading = Get-CountValue -CountsObject $counts -StatusName "loading"
        $merged = Get-CountValue -CountsObject $counts -StatusName "merged"
        $failed = Get-CountValue -CountsObject $counts -StatusName "failed"
        $quarantined = Get-CountValue -CountsObject $counts -StatusName "quarantined"
        Write-Log ("{0} loader_state validated={1} skipped={2} pending={3} claimed={4} loading={5} merged={6} failed={7} quarantined={8} latest_validated={9} latest_failed={10}" -f $streamName, $validated, $skipped, $pending, $claimed, $loading, $merged, $failed, $quarantined, $remoteState.latest_validated_segment, $remoteState.latest_failed_segment)
        if (-not [string]::IsNullOrWhiteSpace($remoteState.latest_failed_error)) {
            Write-Log ("{0} latest_failed_error={1}" -f $streamName, $remoteState.latest_failed_error)
        }
    }

    $needsTrxKick = [string]::IsNullOrWhiteSpace($remote.trx_pid) -and $remote.trx_service_active -ne "activating"
    $needsResKick = [string]::IsNullOrWhiteSpace($remote.res_pid) -and $remote.res_service_active -ne "activating"

    if ($needsTrxKick) {
        Write-Log "ALERT trx_not_running attempting_restart"
        try {
            $out = Ensure-RemoteStream -RemoteHost $instance.PublicIp -ServiceName $streams.trx.ServiceName -TimerName $streams.trx.TimerName
            foreach ($line in $out) {
                Write-Log ("trx_restart> " + $line)
            }
        } catch {
            Write-Log "ALERT trx_restart_failed=$_"
        }
    }

    if ($needsResKick) {
        Write-Log "ALERT resource_not_running attempting_restart"
        try {
            $out = Ensure-RemoteStream -RemoteHost $instance.PublicIp -ServiceName $streams.res.ServiceName -TimerName $streams.res.TimerName
            foreach ($line in $out) {
                Write-Log ("res_restart> " + $line)
            }
        } catch {
            Write-Log "ALERT resource_restart_failed=$_"
        }
    }

    try {
        $probe = Invoke-ClickHouseProbe
        $trx = $probe.trx
        $trxAudit = $probe.trx_audit
        $res = $probe.res
        $resAudit = $probe.res_audit
        if ($null -eq $trx -or $null -eq $trxAudit -or $null -eq $res -or $null -eq $resAudit) {
            throw "clickhouse probe returned null payload"
        }
    } catch {
        Write-Log "ALERT clickhouse_probe_failed=$_"
        if ($MaxIterations -gt 0 -and $iteration -ge $MaxIterations) { break }
        Start-Sleep -Seconds $IntervalSeconds
        continue
    }

    Write-Log ("trx rows={0} blocks={1}->{2} batches={3} inserted_rows={4} last_segment={5}" -f $trx.rows, $trx.min_block, $trx.max_block, $trxAudit.batches, $trxAudit.inserted_rows, $trxAudit.last_segment_id)
    Write-Log ("res rows={0} blocks={1}->{2} batches={3} inserted_rows={4} last_segment={5}" -f $res.rows, $res.min_block, $res.max_block, $resAudit.batches, $resAudit.inserted_rows, $resAudit.last_segment_id)

    foreach ($streamName in @("trx", "res")) {
        $cfg = $streams[$streamName]
        $slot = $state[$streamName]
        $remoteState = $loaderState.$streamName
        $counts = $remoteState.counts
        $validated = Get-CountValue -CountsObject $counts -StatusName "validated"
        $skipped = Get-CountValue -CountsObject $counts -StatusName "skipped"
        $pending = Get-CountValue -CountsObject $counts -StatusName "pending"
        $claimed = Get-CountValue -CountsObject $counts -StatusName "claimed"
        $loading = Get-CountValue -CountsObject $counts -StatusName "loading"
        $merged = Get-CountValue -CountsObject $counts -StatusName "merged"
        $failed = Get-CountValue -CountsObject $counts -StatusName "failed"
        $quarantined = Get-CountValue -CountsObject $counts -StatusName "quarantined"
        $doneSegments = $validated + $skipped
        $activeSegments = $pending + $claimed + $loading + $merged
        $errorSegments = $failed + $quarantined

        if ($streamName -eq "trx") {
            $rowCount = [double]$trx.rows
        } else {
            $rowCount = [double]$res.rows
        }

        $now = Get-Date
        if ($null -ne $slot.PrevTime) {
            $elapsedHours = [math]::Max(0.0001, ($now - $slot.PrevTime).TotalHours)
            $deltaSegments = [double]$doneSegments - [double]$slot.PrevDoneSegments
            $deltaRows = $rowCount - [double]$slot.PrevRows
            $segmentsPerHour = $deltaSegments / $elapsedHours
            $rowsPerHour = $deltaRows / $elapsedHours
            $remainingSegments = [math]::Max(0, $cfg.TotalSegments - $doneSegments)
            Write-Log ("{0} delta_segments={1} delta_rows={2} rate_segments_per_hour={3:N2} rate_rows_per_hour={4:N0}" -f $streamName, $deltaSegments, $deltaRows, $segmentsPerHour, $rowsPerHour)
            if ($segmentsPerHour -gt 0) {
                $etaHours = $remainingSegments / $segmentsPerHour
                Write-Log ("{0} remaining_segments={1} eta_hours={2:N2}" -f $streamName, $remainingSegments, $etaHours)
            } else {
                Write-Log ("ALERT {0}_not_advancing_in_last_window" -f $streamName)
            }
        }

        $slot.PrevDoneSegments = $doneSegments
        $slot.PrevRows = $rowCount
        $slot.PrevTime = $now

        if ($activeSegments -eq 0 -and $errorSegments -gt 0) {
            if ($slot.ReconcilePasses -lt $maxReconcilePasses) {
                $slot.ReconcilePasses++
                Write-Log ("ALERT {0}_reconciliation_pass_start pass={1} failed={2} quarantined={3}" -f $streamName, $slot.ReconcilePasses, $failed, $quarantined)
                try {
                    $requeue = Requeue-RemoteErrors -RemoteHost $instance.PublicIp -StreamConfig $cfg
                    Write-Log ("{0}_requeue affected={1} before={2} after={3}" -f $streamName, $requeue.affected, ($requeue.before | ConvertTo-Json -Compress), ($requeue.after | ConvertTo-Json -Compress))
                    $out = Ensure-RemoteStream -RemoteHost $instance.PublicIp -ServiceName $cfg.ServiceName -TimerName $cfg.TimerName
                    foreach ($line in $out) {
                        Write-Log ("{0}_reconcile_restart> " -f $streamName + $line)
                    }
                } catch {
                    Write-Log ("ALERT {0}_reconciliation_failed={1}" -f $streamName, $_)
                }
            } else {
                Write-Log ("ALERT {0}_reconciliation_exhausted failed={1} quarantined={2}" -f $streamName, $failed, $quarantined)
            }
        }

        $slot.Completed = ($doneSegments -ge $cfg.TotalSegments -and $activeSegments -eq 0 -and $errorSegments -eq 0)
    }

    try {
        $cpu = Get-LatestCloudWatchValue -MetricName "CPUUtilization" -Stats @("Average", "Maximum") -StartTime (Get-Date).AddMinutes(-20)
        $credits = Get-LatestCloudWatchValue -MetricName "CPUCreditBalance" -Stats @("Average", "Minimum", "Maximum") -StartTime (Get-Date).AddHours(-6)
        if ($null -ne $cpu) {
            Write-Log ("cpu_average={0} cpu_maximum={1}" -f (Format-FloatOrNa $cpu.Average), (Format-FloatOrNa $cpu.Maximum))
        }
        if ($null -ne $credits) {
            Write-Log ("cpu_credit_balance_average={0} cpu_credit_balance_minimum={1}" -f (Format-FloatOrNa $credits.Average), (Format-FloatOrNa $credits.Minimum))
        }
    } catch {
        Write-Log "ALERT cloudwatch_probe_failed=$_"
    }

    if ($state.trx.Completed -and $state.res.Completed) {
        Write-Log "FINAL both_streams_completed final_validation_ok"
        try {
            Write-FinalReport -Probe $probe -LoaderState $loaderState
            Write-Log ("FINAL report_written path={0}" -f $finalReportPath)
        } catch {
            Write-Log "ALERT final_report_failed=$_"
        }
        try {
            Stop-TargetInstance
        } catch {
            Write-Log "ALERT instance_stop_failed=$_"
        }
        break
    }

    if ($MaxIterations -gt 0 -and $iteration -ge $MaxIterations) {
        break
    }

    Start-Sleep -Seconds $IntervalSeconds
}
