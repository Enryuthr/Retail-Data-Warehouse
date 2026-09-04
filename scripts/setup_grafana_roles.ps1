$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root ".env"
$values = @{}
Get-Content -LiteralPath $envFile | ForEach-Object {
    if ($_ -match '^([^#=]+)=(.*)$') { $values[$matches[1]] = $matches[2] }
}

function New-Secret {
    $bytes = [byte[]]::new(32)
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', 'A').Replace('/', 'B')
}

$defaults = [ordered]@{
    GRAFANA_PORT = "13002"
    GRAFANA_ADMIN_USER = "admin"
    GRAFANA_ADMIN_PASSWORD = New-Secret
    GRAFANA_RETAIL_DB_USER = "grafana_retail_reader"
    GRAFANA_RETAIL_DB_PASSWORD = New-Secret
    GRAFANA_AIRFLOW_DB_USER = "grafana_airflow_reader"
    GRAFANA_AIRFLOW_DB_PASSWORD = New-Secret
}
$missing = foreach ($key in $defaults.Keys) {
    if (-not $values.ContainsKey($key) -or [string]::IsNullOrWhiteSpace($values[$key])) {
        "$key=$($defaults[$key])"
    }
}
if ($missing) {
    Add-Content -LiteralPath $envFile -Value ("`r`n" + ($missing -join "`r`n"))
    $missing | ForEach-Object {
        $parts = $_ -split '=', 2
        $values[$parts[0]] = $parts[1]
    }
}

$env:GRAFANA_RETAIL_DB_PASSWORD = $values.GRAFANA_RETAIL_DB_PASSWORD
$env:PGPASSWORD = $values.POSTGRES_PASSWORD
$psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"
& $psql -X -v ON_ERROR_STOP=1 -h localhost -p $values.POSTGRES_PORT -U $values.POSTGRES_USER -d $values.POSTGRES_DB -f (Join-Path $root "grafana\sql\retail_reader.sql")

$env:GRAFANA_AIRFLOW_DB_PASSWORD = $values.GRAFANA_AIRFLOW_DB_PASSWORD
Get-Content -Raw (Join-Path $root "grafana\sql\airflow_reader.sql") |
    docker compose -f (Join-Path $root "docker-compose.airflow.yml") exec -T -e GRAFANA_AIRFLOW_DB_PASSWORD airflow-postgres psql -X -v ON_ERROR_STOP=1 -U airflow -d airflow

Write-Host "Grafana local secrets are present in .env and both read-only roles are configured."
