param(
  [Parameter(Mandatory = $true, Position = 0)]
  [string]$Topic,

  [int]$TimeoutMs = 10000
)

docker exec kafka-cdc kafka-console-consumer `
  --bootstrap-server kafka-cdc:9092 `
  --topic $Topic `
  --from-beginning `
  --timeout-ms $TimeoutMs `
  --property print.timestamp=true `
  --property print.partition=true `
  --property print.offset=true `
  --property print.key=true 2>$null