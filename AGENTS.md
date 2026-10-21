# AGENTS.md — Botmaker Log Ingestion Pipeline

Guía de contexto para agentes de IA y desarrolladores. Describe la API, el
esquema de datos y las convenciones del proyecto. La información aquí fue
verificada contra el stack desplegado (`BotmakerLogIngestionDev`) y el código
fuente, no contra plantillas sin usar.

## Qué es este proyecto

Pipeline serverless que recibe webhooks de Botmaker, los normaliza a un esquema
plano de 16 campos, los convierte a Parquet vía Amazon Data Firehose y los
almacena en S3 con particiones diarias, consultables con Athena.

Flujo: `API Gateway (HTTP API) → Lambda (ingesta/normalización/consulta) →
Firehose (JSON→Parquet) → S3 → Athena`.

## Infraestructura: CDK, no SAM

- **El IaC oficial es AWS CDK**, en `infra/` (`infra/app.py`,
  `infra/stacks/ingestion_stack.py`, `infra/stacks/pipeline_stack.py`).
- El stack desplegado es `BotmakerLogIngestionDev`, creado por el CDK app.
- **No existe `template.yaml`.** Hubo una plantilla SAM en la raíz que nunca se
  usó para desplegar; se eliminó para evitar confusión. Si ves referencias a
  SAM, son legacy.
- **Pendiente (migración de CI/CD a CDK):** el pipeline en `buildspecs/` y
  `pipeline/codepipeline.yaml` todavía contiene comandos SAM (`sam build`,
  `sam package`). Deben migrarse a `cdk synth` / `cdk deploy`. Los puntos
  exactos están marcados con `TODO (CDK migration)` en `buildspecs/buildspec-ci.yml`
  y `buildspecs/buildspec-package.yml`.

## Ambiente

- Desplegado **solo en `dev`** (cuenta `423623851097`, región `us-east-1`).
  No existe ni se planea `prod`.
- A pesar del sufijo `dev`, es el ambiente **real y productivo**: el webhook de
  Botmaker apunta aquí y recibe tráfico en vivo. No tratar como datos de prueba.
- Recursos reales:
  - API: `https://5g907s87s5.execute-api.us-east-1.amazonaws.com`
  - Lambda: `botmaker-log-ingestion-dev` (Python 3.12, arm64, 256 MB, timeout 10s)
  - Base de datos Glue/Athena: `botmaker_log_ingestion_dev`
  - Tabla: `botmaker_logs`
  - WorkGroup Athena: `botmaker-log-ingestion-dev`
  - Firehose: `botmaker-log-ingestion-dev`
  - Secret (Bearer token): `botmaker-log-ingestion/dev/webhook-secret`

## Autenticación

Todas las rutas (incluido `GET /webhook/health`) requieren un header
`Authorization: Bearer <token>`. El token se guarda en Secrets Manager y se
compara en tiempo constante (`hmac.compare_digest`). Sin token o con token
inválido → `401`.

## Referencia de la API

Las 4 rutas están definidas en el CDK (`ingestion_stack.py`) y enrutadas
internamente por `src/handler.py`.

### `POST /webhook` — Ingesta de eventos

Recibe el payload crudo de Botmaker, lo normaliza y lo entrega a Firehose.

- Límites: payload crudo ≤ 6 MB (`413` si se excede), body decodificado ≤ 1 MB
  (`400` si se excede). Soporta base64.
- Validaciones: JSON parseable (`400` si no), debe ser objeto JSON (`400` si es
  array/primitivo), campos requeridos derivables (`400` si falta `environment`
  o no se puede derivar `event_type`/`event_id`).
- Entrega a Firehose con reintentos; si falla → `503`.

Respuesta `202`:

```json
{
  "status": "accepted",
  "ingestion_id": "15a57745-ddd6-46ad-bf18-ea66df64a03b",
  "event_id": "err_...",
  "event_type": "EXTERNAL_API_ERROR",
  "fields_extracted": {
    "conversation_id": "...", "message_id": "...", "user_id": "...",
    "order_form_id": "...", "source_channel": "...", "source_bot": "...",
    "severity": "...", "error_code": "..."
  }
}
```

### `GET /webhook/health` — Health check

Respuesta `200`: `{"status": "healthy", "environment": "dev", "firehose_stream": "botmaker-log-ingestion-dev"}`.

### `GET /webhook/{ingestion_id}` — Buscar un evento por id

1. Revisa CloudWatch Logs por `FIREHOSE_ACCEPTED` (confirma procesamiento,
   ventana de 10 min).
2. Consulta Athena por el registro Parquet almacenado.

Respuestas: `200 stored` (en S3), `200 pending` (aceptado pero aún en buffer de
Firehose ~5 min), `404 not_found`, `400` si el id tiene < 10 caracteres.

### `GET /webhook?date=YYYY-MM-DD` — Listar eventos por fecha

Consulta Athena filtrando por `ingestion_date`. Filtros opcionales:
`event_type`, `conversation_id`, `source_channel`, `limit` (máx. 100).

Respuesta `200`: `{"status": "success", "date": "...", "filters": {...}, "count": N, "records": [...]}`.
Si la query de Athena falla → `503`.

## Esquema del registro almacenado (16 campos + partición)

Cada registro se guarda como Parquet. Todos los campos vuelven como string
desde Athena (incluido `schema_version`, cuyo tipo en Glue es `int`).

| # | Campo | Tipo (Glue) | Requerido | Origen desde el payload |
|---|-------|-------------|-----------|-------------------------|
| 1 | `ingestion_id` | string | sí | UUID v4 generado en la ingesta (clave única) |
| 2 | `event_id` | string | sí | `eventId` → `errorId` → `linkId` → `messageId` → `conversation.messageId` → UUID v4 |
| 3 | `request_id` | string | no | Request ID de API Gateway (correlación) |
| 4 | `received_at` | string | sí | Timestamp UTC de recepción (`YYYY-MM-DDTHH:MM:SS.mmmZ`) |
| 5 | `schema_version` | int | sí | Constante `SCHEMA_VERSION` (actualmente `1`) |
| 6 | `environment` | string | sí | `dev` (de la config del Lambda) |
| 7 | `event_type` | string | sí | `eventType` → `error.type` → `type` → `PAYMENT_LINK_EVENT` (si hay `paymentLink`) → `ORDER_EVENT` (si hay `orders`) → `UNKNOWN` |
| 8 | `conversation_id` | string | no | `conversation.conversationId` |
| 9 | `message_id` | string | no | `messageId` → `conversation.messageId` |
| 10 | `user_id` | string | no | `conversation.userId` |
| 11 | `order_form_id` | string | no | `orderForm.id` → `context.orderFormId` |
| 12 | `source_channel` | string | no | `source.channel` |
| 13 | `source_bot` | string | no | `source.bot` |
| 14 | `severity` | string | no | `error.severity` |
| 15 | `error_code` | string | no | `error.code` |
| 16 | `raw_event_json` | string | sí | Payload original completo serializado como JSON (sin pérdida) |
| — | `ingestion_date` | string | sí (partición) | Fecha de llegada a Firehose, zona `America/Bogota` |

Notas:
- Los campos 8–15 son opcionales: si el payload no los trae, quedan `null`.
- `raw_event_json` preserva el 100% del payload; cualquier campo no mapeado se
  consulta con `json_extract_scalar(raw_event_json, '$.ruta.al.campo')`.
- `ingestion_date` usa la hora de llegada a Firehose (UTC-5 Bogotá), no el
  timestamp del evento. Un evento de 23:30 UTC puede caer en la partición del
  día siguiente en Bogotá.

## Tipos de evento observados en datos reales

`PAYMENT_LINK_EVENT`, `EXTERNAL_API_ERROR`, `INTERNAL_ERROR`, `MESSAGE_RECEIVED`.
Los eventos de smoke test usan `event_type = 'PIPELINE_SMOKE_TEST'` y deben
excluirse de analítica.

## Payloads crudos de entrada (ejemplos reales)

### Evento de error (`EXTERNAL_API_ERROR`)

```json
{
  "errorId": "err_001",
  "timestamp": "2026-08-05T20:30:00.000Z",
  "source": { "channel": "whatsapp", "bot": "agentcore" },
  "conversation": { "conversationId": "conv_001", "userId": "user_001", "phone": "********1234", "messageId": "wamid.001" },
  "error": { "type": "EXTERNAL_API_ERROR", "code": "CATALOG_001", "message": "...", "severity": "ERROR" },
  "context": { "operation": "getProductAvailability", "orderFormId": "of_001" }
}
```

### Link de pago (`PAYMENT_LINK_EVENT`)

```json
{
  "linkId": "plink_001",
  "timestamp": "2026-08-05T20:35:00.000Z",
  "store": { "account": "tottoco", "domain": "https://co.totto.com" },
  "orderForm": { "id": "of_002" },
  "paymentLink": { "status": "ACTIVE", "url": "https://.../payment/001" },
  "conversation": { "conversationId": "conv_002", "userId": "user_002", "messageId": "wamid.002" },
  "orders": [ { "orderId": "243468274-01", "total": 25000000 } ],
  "source": { "channel": "whatsapp", "bot": "agentcore" }
}
```

### Evento de orden (`ORDER_EVENT`)

Se detecta por la presencia de la clave `orders` cuando no hay un `event_type`
más específico.

## Trabajo futuro: evento de "Transacción completada" (propuesto)

Objetivo: saber qué conversaciones con link de pago generado efectivamente
completaron el pago. En lugar de "actualizar" un registro (los datos son
Parquet inmutable), se emite un **evento nuevo** (p. ej. `PAYMENT_COMPLETED`)
vía el mismo `POST /webhook`, y el cruce link↔pago se hace en consulta.

**Clave de correlación decidida: `order_form_id`.** Justificación con datos
reales (627 registros a oct-2026):
- `PAYMENT_LINK_EVENT` trae `order_form_id` el **100%** de las veces.
- `order_form_id` es prácticamente único por transacción (355 links → 354 ids
  distintos) y **nunca se comparte entre conversaciones distintas**.
- Una misma `conversation_id` puede tener **varios** `order_form_id` (se observó
  hasta 6 links en una sola conversación), por lo que `conversation_id` NO
  sirve como clave de cruce: sería ambiguo.

Pendiente de definir con el equipo de Botmaker antes de crear la spec:
- Contrato exacto del payload del evento de pago (debe incluir `order_form_id`
  en `orderForm.id` o `context.orderFormId`).
- Si se promueven campos nuevos a columnas (`payment_status`, `amount`,
  `currency`, `paid_at`) o se dejan en `raw_event_json`.

## Desarrollo local

```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements-dev.txt
pytest tests/unit/ tests/properties/ tests/schema/ -v
```

Nota: el runtime del Lambda es Python 3.12. Correr los tests con una versión
distinta (p. ej. 3.14) puede provocar fallos de mocking en `tests/unit/test_handler.py`.
