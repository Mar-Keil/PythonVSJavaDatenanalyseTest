$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonDir = Join-Path $scriptDir 'python'
$venvDir = Join-Path $pythonDir '.venv'
$polarsBench = Join-Path $venvDir 'Scripts\polars-benchmarks.exe'
$pandasBench = Join-Path $venvDir 'Scripts\pandas-benchmarks.exe'
$jarPath = Join-Path $scriptDir 'java\target\java-0.1.0-SNAPSHOT-benchmarks.jar'

function Test-Java17Home {
    param([string]$JavaHome)

    if (-not $JavaHome) {
        return $false
    }

    $candidateExe = Join-Path $JavaHome 'bin\java.exe'
    if (-not (Test-Path -LiteralPath $candidateExe)) {
        return $false
    }

    $versionOutput = & $candidateExe -version 2>&1
    if ($LASTEXITCODE -ne 0) {
        return $false
    }

    $versionText = ($versionOutput | Out-String)
    return $versionText -match 'version "17(\.|")'
}

function Resolve-Java17Home {
    if (Test-Java17Home -JavaHome $env:JAVA_HOME) {
        return $env:JAVA_HOME
    }

    $candidates = @(
        'C:\Program Files\Eclipse Adoptium\jdk-17*',
        "$env:USERPROFILE\.jdks\temurin-17*",
        "$env:USERPROFILE\.jdks\corretto-17*"
    )

    foreach ($pattern in $candidates) {
        foreach ($candidate in @(Get-Item $pattern -ErrorAction SilentlyContinue | Select-Object -ExpandProperty FullName)) {
            if (Test-Java17Home -JavaHome $candidate) {
                return $candidate
            }
        }
    }

    return $null
}

$javaHome = Resolve-Java17Home
if (-not $javaHome) {
    throw 'No JDK 17 installation found. Install JDK 17 or set JAVA_HOME to a JDK 17 directory.'
}

$env:JAVA_HOME = $javaHome
$javaExe = Join-Path $javaHome 'bin\java.exe'

if (-not (Test-Path -LiteralPath $polarsBench)) {
    throw "Polars benchmark entry point not found: $polarsBench"
}

if (-not (Test-Path -LiteralPath $pandasBench)) {
    throw "Pandas benchmark entry point not found: $pandasBench"
}

& mvn -f (Join-Path $scriptDir 'java\pom.xml') clean package
if ($LASTEXITCODE -ne 0) {
    throw 'Maven build failed.'
}

& $javaExe -Xmx14g -jar $jarPath
if ($LASTEXITCODE -ne 0) {
    throw 'Java benchmarks failed.'
}

& $polarsBench
if ($LASTEXITCODE -ne 0) {
    throw 'Polars benchmarks failed.'
}

& $pandasBench
if ($LASTEXITCODE -ne 0) {
    throw 'Pandas benchmarks failed.'
}
