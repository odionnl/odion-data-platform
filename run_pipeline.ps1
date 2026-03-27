# Dagelijkse ETL + dbt run voor verantwoording feitelijk geleverde zorg
# Uitvoeren vanuit de project root (zodat .dlt/secrets.toml gevonden wordt)

$projectDir = $PSScriptRoot
$logFile = "$projectDir\logs\pipeline_$(Get-Date -Format 'yyyy-MM-dd').log"

Set-Location $projectDir

function Log($msg) {
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $msg"
    Write-Host $line
    Add-Content -Path $logFile -Value $line
}

Log "=== Start dagelijkse pipeline ==="

# Stap 1: ORTEC laden
Log "Stap 1: ORTEC ingestion starten..."
poetry run python -m ingestion.pipelines.ortec 2>&1 | Tee-Object -Append -FilePath $logFile
if ($LASTEXITCODE -ne 0) {
    Log "FOUT: ORTEC ingestion mislukt. Pipeline gestopt."
    exit 1
}
Log "Stap 1 klaar."

# Stap 2: Audit-bestanden laden
Log "Stap 2: Ons audits ingestion starten..."
poetry run python -m ingestion.pipelines.ons_audits 2>&1 | Tee-Object -Append -FilePath $logFile
if ($LASTEXITCODE -ne 0) {
    Log "FOUT: Audit ingestion mislukt. Pipeline gestopt."
    exit 1
}
Log "Stap 2 klaar."

# Stap 3: dbt run (alle modellen)
Log "Stap 3: dbt run starten..."
poetry run dbt run 2>&1 | Tee-Object -Append -FilePath $logFile
if ($LASTEXITCODE -ne 0) {
    Log "FOUT: dbt run mislukt."
    exit 1
}
Log "Stap 3 klaar."

Log "=== Pipeline succesvol afgerond ==="
