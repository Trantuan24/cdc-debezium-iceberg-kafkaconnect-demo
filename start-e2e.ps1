param(
    [switch]$Reset
)

$ErrorActionPreference = "Stop"

if ($Reset) {
    Write-Host "Removing containers and demo volumes..." -ForegroundColor Yellow
    docker compose down -v
} else {
    Write-Host "Keeping existing database volumes. Init scripts only run on new volumes." -ForegroundColor Yellow
}

Write-Host "Starting the Docker Compose stack..." -ForegroundColor Green
docker compose up --build -d

function Wait-Until([string]$Label, [scriptblock]$Check, [int]$TimeoutSeconds, [int]$IntervalSeconds = 5) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (& $Check) {
            Write-Host "$Label is ready." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds $IntervalSeconds
    }
    throw "Timed out waiting for $Label after $TimeoutSeconds seconds."
}

Wait-Until "Oracle" {
    $status = docker inspect -f '{{.State.Health.Status}}' oracle 2>$null
    $status -eq "healthy"
} 600 10

Wait-Until "Trino query engine" {
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    docker exec trino trino --execute "SELECT 1" 2>$null | Out-Null
    $trinoExitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorAction
    $trinoExitCode -eq 0
} 300 5

$icebergSchemas = @(
    "mysql_mydb",
    "postgres_mydb_pg_public",
    "mongo_mydb_mongo",
    "oracle_xepdb1_debezium"
)

Write-Host "Ensuring source-aligned Iceberg schemas exist..." -ForegroundColor Green
foreach ($schema in $icebergSchemas) {
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.$schema" 2>$null | Out-Null
    $trinoExitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorAction
    if ($trinoExitCode -ne 0) {
        throw "Failed to create Iceberg schema $schema"
    }
}
# ORACLE_ENABLE_ARCHIVELOG=true configures ARCHIVELOG during initial creation.
Wait-Until "Kafka Connect API" {
    $code = curl.exe -s -o NUL -w "%{http_code}" http://localhost:8083/connectors
    $code -eq "200"
} 300 5

$connectorFiles = @(
    "debezium-mysql-raw-source.json",
    "debezium-postgres-raw-source.json",
    "debezium-mongodb-raw-source.json",
    "debezium-oracle-raw-source.json",
    "iceberg-sink-raw-mysql-orders.json",
    "iceberg-sink-raw-mysql-customers.json",
    "iceberg-sink-raw-postgres-inventory.json",
    "iceberg-sink-raw-mongodb-products.json",
    "iceberg-sink-raw-oracle-transactions.json",
    "iceberg-append-raw-mysql-orders.json",
    "iceberg-append-raw-mysql-customers.json",
    "iceberg-append-raw-postgres-inventory.json",
    "iceberg-append-raw-mongodb-products.json",
    "iceberg-append-raw-oracle-transactions.json"
)

Write-Host "Applying all source and sink connector configurations..." -ForegroundColor Green
foreach ($file in $connectorFiles) {
    $path = Join-Path $PSScriptRoot "connectors/$file"
    $payload = Get-Content -Raw -LiteralPath $path | ConvertFrom-Json
    $name = $payload.name
    $escapedName = [Uri]::EscapeDataString($name)
    $existingStatus = curl.exe -s -o NUL -w "%{http_code}" "http://localhost:8083/connectors/$escapedName"

    if ($existingStatus -eq "200") {
        Write-Host "Updating $name from $file..."
        $body = $payload.config | ConvertTo-Json -Depth 20 -Compress
        Invoke-RestMethod -Method Put `
            -Uri "http://localhost:8083/connectors/$escapedName/config" `
            -ContentType "application/json" `
            -Body $body | Out-Null
    } elseif ($existingStatus -eq "404") {
        Write-Host "Creating $name from $file..."
        $body = $payload | ConvertTo-Json -Depth 20 -Compress
        Invoke-RestMethod -Method Post `
            -Uri "http://localhost:8083/connectors" `
            -ContentType "application/json" `
            -Body $body | Out-Null
    } else {
        throw "Unexpected HTTP $existingStatus while checking connector $name"
    }
}

Write-Host "All connector configurations are applied. Verify tasks at http://localhost:8083/connectors?expand=status" -ForegroundColor Green