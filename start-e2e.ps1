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

# ORACLE_ENABLE_ARCHIVELOG=true configures ARCHIVELOG during first database
# creation. Do not restart/mount the database again here.
Wait-Until "Kafka Connect API" {
    $code = curl.exe -s -o NUL -w "%{http_code}" http://localhost:8083/connectors
    $code -eq "200"
} 300 5

Write-Host "Registering all source and sink connectors..." -ForegroundColor Green
$connectorFiles = @(
    "debezium-source.json",
    "debezium-postgres-source.json",
    "debezium-mongodb-source.json",
    "debezium-oracle-source.json",
    "iceberg-sink.json",
    "iceberg-sink-postgres.json",
    "iceberg-sink-mongodb.json",
    "iceberg-sink-oracle.json"
)

foreach ($file in $connectorFiles) {
    Write-Host "Registering $file..."
    $responseFile = New-TemporaryFile
    try {
        $statusCode = curl.exe -sS -o $responseFile.FullName -w "%{http_code}" `
            -X POST http://localhost:8083/connectors `
            -H "Content-Type: application/json" `
            -d "@connectors/$file"
        $body = Get-Content -Raw $responseFile.FullName
        if ([int]$statusCode -ge 300) {
            throw "Connector registration failed for $file (HTTP $statusCode): $body"
        }
        Write-Host $body
    } finally {
        Remove-Item -LiteralPath $responseFile.FullName -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "All connectors registered. Verify task state via http://localhost:8083/connectors?expand=status" -ForegroundColor Green