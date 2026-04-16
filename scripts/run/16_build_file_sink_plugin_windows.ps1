$ErrorActionPreference = 'Stop'

$workspaceRoot = 'G:\CODEX\LOCALNODA\local-tron-usdt-backfill'
$javaHome = 'C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot'
$gradleBat = 'G:\CODEX\LOCALNODA\_tools\gradle-8.7\bin\gradle.bat'
$pluginRoot = Join-Path $workspaceRoot 'extractor\plugin'
$pluginZip = Join-Path $workspaceRoot 'artifacts\plugins\plugin-file-sink.zip'

if (-not (Test-Path $javaHome)) {
    throw "JAVA_HOME not found at $javaHome"
}

if (-not (Test-Path $gradleBat)) {
    throw "Gradle not found at $gradleBat"
}

$env:JAVA_HOME = $javaHome
$env:Path = "$javaHome\bin;G:\CODEX\LOCALNODA\_tools\gradle-8.7\bin;$env:Path"

Push-Location $pluginRoot
try {
    & $gradleBat clean assembleWorkspacePlugin
} finally {
    Pop-Location
}

if (-not (Test-Path $pluginZip)) {
    throw "Build did not produce $pluginZip"
}

Get-Item $pluginZip | Format-List FullName,Length,LastWriteTime
