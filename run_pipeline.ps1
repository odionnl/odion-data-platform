# Dagelijkse ETL + dbt run voor verantwoording feitelijk geleverde zorg
# Uitvoeren vanuit de project root (zodat .dlt/secrets.toml gevonden wordt)

$projectDir = $PSScriptRoot
$logFile = "$projectDir\logs\pipeline_$(Get-Date -Format 'yyyy-MM-dd').log"

Set-Location $projectDir

function Log($msg) {
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $msg"
    Write-Host $line
    Add-Content -Path $logFile -Value $line -Encoding utf8
}

function Run-Step($command) {
    Invoke-Expression $command 2>&1 | ForEach-Object {
        Write-Host $_
        Add-Content -Path $logFile -Value $_ -Encoding utf8
    }
}

Log "=== Start dagelijkse pipeline ==="

# Stap 0: code bijwerken naar laatste versie van main
Log "Stap 0: git main ophalen..."
$codeActueel = $true
Run-Step "git switch main"
if ($LASTEXITCODE -eq 0) {
    Run-Step "git pull --ff-only"
}
if ($LASTEXITCODE -ne 0) {
    $codeActueel = $false
    Log "WAARSCHUWING: kon main niet ophalen; pipeline draait met huidige code ($(git branch --show-current)), cleanup wordt overgeslagen."
} else {
    Log "Stap 0 klaar."
}

# Stap 1: ORTEC laden
Log "Stap 1: ORTEC ingestion starten..."
Run-Step "poetry run python -m ingestion.pipelines.ortec"
if ($LASTEXITCODE -ne 0) {
    Log "FOUT: ORTEC ingestion mislukt. Pipeline gestopt."
    exit 1
}
Log "Stap 1 klaar."

# Stap 2: Audit-bestanden laden
Log "Stap 2: Ons audits ingestion starten..."
Run-Step "poetry run python -m ingestion.pipelines.ons_audits"
if ($LASTEXITCODE -ne 0) {
    Log "FOUT: Audit ingestion mislukt. Pipeline gestopt."
    exit 1
}
Log "Stap 2 klaar."

# Stap 3: dbt run (alle modellen)
Log "Stap 3: dbt run starten..."
Run-Step "poetry run dbt run"
if ($LASTEXITCODE -ne 0) {
    Log "FOUT: dbt run mislukt."
    exit 1
}
Log "Stap 3 klaar."

# Stap 4: opruimen van objecten die niet meer in het dbt-project staan
# Alleen op actuele main, anders worden modellen die alleen op main bestaan weggegooid
if (-not $codeActueel) {
    Log "Stap 4 overgeslagen: code is niet actueel met main."
} else {
    Log "Stap 4: Cleanup stale modellen..."
    Run-Step "poetry run dbt run-operation clean_stale_models --args '{dryrun: false}'"
    if ($LASTEXITCODE -ne 0) {
        Log "WAARSCHUWING: cleanup mislukt (niet fataal; data is intact)."
    } else {
        Log "Stap 4 klaar."
    }
}

Log "=== Pipeline succesvol afgerond ==="
