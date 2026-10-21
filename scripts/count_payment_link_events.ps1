<#
.SYNOPSIS
    Cuenta el total de eventos PAYMENT_LINK_EVENT almacenados en la tabla
    botmaker_logs (todos los registros, sin filtro de fecha).

.DESCRIPTION
    Ejecuta una consulta Athena contra la base de datos del pipeline de
    ingesta de logs de Botmaker y devuelve el conteo total de eventos cuyo
    event_type es 'PAYMENT_LINK_EVENT'.

    Requiere AWS CLI v2 configurado con credenciales que tengan permisos de
    Athena (StartQueryExecution, GetQueryExecution, GetQueryResults) y acceso
    de lectura al bucket de resultados y al catálogo de Glue.

.PARAMETER Database
    Base de datos Glue/Athena. Por defecto: botmaker_log_ingestion_dev

.PARAMETER WorkGroup
    WorkGroup de Athena. Por defecto: botmaker-log-ingestion-dev

.PARAMETER EventType
    Tipo de evento a contar. Por defecto: PAYMENT_LINK_EVENT

.EXAMPLE
    ./scripts/count_payment_link_events.ps1

.EXAMPLE
    ./scripts/count_payment_link_events.ps1 -EventType EXTERNAL_API_ERROR

.NOTES
    Ambiente: el pipeline está desplegado solo en 'dev' (cuenta 423623851097,
    us-east-1), que es el ambiente real/productivo. Ver README.md y AGENTS.md.
#>
[CmdletBinding()]
param(
    [string]$Database = "botmaker_log_ingestion_dev",
    [string]$WorkGroup = "botmaker-log-ingestion-dev",
    [string]$EventType = "PAYMENT_LINK_EVENT"
)

$ErrorActionPreference = "Stop"

$query = "SELECT count(*) AS total FROM botmaker_logs WHERE event_type = '$EventType'"

Write-Host "Ejecutando consulta Athena..."
Write-Host "  Database:  $Database"
Write-Host "  WorkGroup: $WorkGroup"
Write-Host "  Query:     $query"
Write-Host ""

# 1) Iniciar la ejecución de la consulta
$queryExecutionId = aws athena start-query-execution `
    --query-string $query `
    --query-execution-context "Database=$Database" `
    --work-group $WorkGroup `
    --query "QueryExecutionId" `
    --output text

if (-not $queryExecutionId) {
    throw "No se pudo iniciar la consulta Athena."
}

# 2) Esperar a que termine (polling, máximo 60s)
$deadline = (Get-Date).AddSeconds(60)
do {
    Start-Sleep -Seconds 2
    $state = aws athena get-query-execution `
        --query-execution-id $queryExecutionId `
        --query "QueryExecution.Status.State" `
        --output text

    if ($state -eq "SUCCEEDED") { break }
    if ($state -in @("FAILED", "CANCELLED")) {
        $reason = aws athena get-query-execution `
            --query-execution-id $queryExecutionId `
            --query "QueryExecution.Status.StateChangeReason" `
            --output text
        throw "La consulta terminó en estado $state. Motivo: $reason"
    }
} while ((Get-Date) -lt $deadline)

if ($state -ne "SUCCEEDED") {
    throw "Timeout: la consulta no terminó en 60 segundos (estado: $state)."
}

# 3) Obtener el resultado (fila 2, el header es la fila 1)
$total = aws athena get-query-results `
    --query-execution-id $queryExecutionId `
    --query "ResultSet.Rows[1].Data[0].VarCharValue" `
    --output text

Write-Host "Total de eventos '$EventType': $total"
