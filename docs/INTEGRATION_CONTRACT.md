# Contrato de Integración — Botmaker Log Ingestion

## Resumen

Este servicio recibe eventos webhook de Botmaker mediante HTTP POST, los normaliza y almacena en formato Parquet para consulta analítica. Cualquier JSON object válido será aceptado y almacenado sin pérdida de información. Adicionalmente expone endpoints GET para debugging y consulta de eventos almacenados.

---

## Endpoints

| Método | Ruta | Descripción |
|--------|------|-------------|
| POST | `/webhook` | Recibir y almacenar un evento |
| GET | `/webhook/health` | Estado del servicio |
| GET | `/webhook/{ingestion_id}` | Consultar un evento por ID |
| GET | `/webhook?date=YYYY-MM-DD` | Listar eventos del día |
| GET | `/webhook?date=YYYY-MM-DD&event_type=X` | Listar eventos filtrados |

**Base URL:** `https://5g907s87s5.execute-api.us-east-1.amazonaws.com`

---

## Autenticación

Todas las peticiones (POST y GET) deben incluir el header `Authorization` con un Bearer token válido.

```
Authorization: Bearer <token>
```

El token es proporcionado por el equipo de infraestructura. Sin este header, la petición será rechazada con HTTP 401.

---

## POST /webhook — Enviar un evento

### Request

```bash
curl -X POST https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "errorId": "err_01JXYZ123",
    "timestamp": "2026-07-28T17:35:42.123Z",
    "source": {"channel": "whatsapp", "bot": "agentcore"},
    "conversation": {
      "conversationId": "conv_123456",
      "userId": "user_987654",
      "messageId": "wamid.xxx"
    },
    "error": {
      "type": "EXTERNAL_API_ERROR",
      "code": "CATALOG_001",
      "severity": "ERROR"
    }
  }'
```

### Response (202 Accepted)

```json
{
  "status": "accepted",
  "ingestion_id": "b689328b-96e0-4e60-bd32-14c9c307f7cf",
  "event_id": "err_01JXYZ123",
  "event_type": "EXTERNAL_API_ERROR",
  "fields_extracted": {
    "conversation_id": "conv_123456",
    "message_id": "wamid.xxx",
    "user_id": "user_987654",
    "order_form_id": null,
    "source_channel": "whatsapp",
    "source_bot": "agentcore",
    "severity": "ERROR",
    "error_code": "CATALOG_001"
  }
}
```

La respuesta muestra inmediatamente qué campos se extrajeron. Guarda el `ingestion_id` para consultar después si el evento llegó a almacenamiento.

### Restricciones del body

| Restricción | Límite |
|-------------|--------|
| Tamaño máximo del body | 6 MB |
| Tamaño máximo decodificado | 1 MB |
| Formato | JSON Object (`{...}`) |
| Encoding | UTF-8 |

---

## GET /webhook/health — Estado del servicio

```bash
curl https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook/health \
  -H "Authorization: Bearer <token>"
```

```json
{"status": "healthy", "environment": "dev", "firehose_stream": "botmaker-log-ingestion-dev"}
```

---

## GET /webhook/{ingestion_id} — Consultar un evento

Permite verificar si un evento se procesó y almacenó. Usa el `ingestion_id` devuelto por el POST.

```bash
curl https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook/b689328b-96e0-4e60-bd32-14c9c307f7cf \
  -H "Authorization: Bearer <token>"
```

### Responses posibles

**Si ya está en Parquet (~5 min después del POST):**
```json
{
  "status": "stored",
  "ingestion_id": "b689328b-...",
  "stored_at": "2026-08-04",
  "record": { ...los 16 campos del Parquet... }
}
```

**Si fue aceptado pero Firehose aún no lo entregó:**
```json
{
  "status": "pending",
  "ingestion_id": "b689328b-...",
  "message": "Event accepted but not yet delivered to storage. Firehose buffers ~5 minutes."
}
```

**Si no se encuentra:**
```json
{
  "status": "not_found",
  "ingestion_id": "xxx",
  "message": "No event found with this ingestion_id"
}
```

---

## GET /webhook?date=YYYY-MM-DD — Listar eventos

Consulta eventos almacenados por fecha. Paginado a máximo 100 registros.

```bash
# Todos los eventos del día
curl "https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook?date=2026-08-04" \
  -H "Authorization: Bearer <token>"

# Filtrado por tipo
curl "https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook?date=2026-08-04&event_type=ORDER_EVENT" \
  -H "Authorization: Bearer <token>"

# Filtrado por conversación
curl "https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook?date=2026-08-04&conversation_id=conv_123456" \
  -H "Authorization: Bearer <token>"
```

### Parámetros

| Parámetro | Requerido | Descripción |
|-----------|-----------|-------------|
| `date` | Sí | Fecha YYYY-MM-DD |
| `event_type` | No | Filtrar por tipo de evento |
| `conversation_id` | No | Filtrar por conversación |
| `source_channel` | No | Filtrar por canal |
| `limit` | No | Máximo registros (default 100) |

### Response

```json
{
  "status": "success",
  "date": "2026-08-04",
  "filters": {"event_type": "ORDER_EVENT"},
  "count": 3,
  "records": [...]
}
```

---

## Campos Reconocidos

### event_id (prioridad)
1. `eventId` → 2. `errorId` → 3. `linkId` → 4. `messageId` → 5. `conversation.messageId` → 6. UUID auto

### event_type (prioridad)
1. `eventType` → 2. `error.type` → 3. `type` → 4. `PAYMENT_LINK_EVENT` (si paymentLink) → 5. `ORDER_EVENT` (si orders) → 6. `UNKNOWN`

### Otros campos

| Columna | Fuente |
|---------|--------|
| `conversation_id` | `conversation.conversationId` |
| `message_id` | `messageId` o `conversation.messageId` |
| `user_id` | `conversation.userId` |
| `order_form_id` | `orderForm.id` o `context.orderFormId` |
| `source_channel` | `source.channel` |
| `source_bot` | `source.bot` |
| `severity` | `error.severity` |
| `error_code` | `error.code` |

---

## Ejemplo — Link de Pago

```bash
curl -X POST https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "linkId": "plink_01JXYZ123",
    "store": {"account": "tottoco", "domain": "https://co.totto.com"},
    "orderForm": {"id": "abc123"},
    "paymentLink": {"status": "ACTIVE"},
    "conversation": {"conversationId": "conv_123456", "userId": "user_987654", "messageId": "wamid.xxx"},
    "orders": [{"orderId": "243468274-01", "total": 25000000}],
    "source": {"channel": "whatsapp", "bot": "agentcore"}
  }'
```

```json
{
  "status": "accepted",
  "ingestion_id": "7f2d1a3b-...",
  "event_id": "plink_01JXYZ123",
  "event_type": "PAYMENT_LINK_EVENT",
  "fields_extracted": {
    "conversation_id": "conv_123456",
    "message_id": "wamid.xxx",
    "user_id": "user_987654",
    "order_form_id": "abc123",
    "source_channel": "whatsapp",
    "source_bot": "agentcore",
    "severity": null,
    "error_code": null
  }
}
```

---

## Códigos de Respuesta

| Código | Significado |
|--------|-------------|
| `200` | Consulta GET exitosa |
| `202` | Evento POST aceptado |
| `400` | JSON inválido, body es array, o falta parámetro date |
| `401` | Token inválido o ausente |
| `404` | Evento no encontrado (GET por ID) |
| `413` | Body > 6 MB |
| `503` | Error temporal — reintentar |

---

## Flujo de Debugging

1. **POST** → guardar `ingestion_id` de la respuesta
2. **Revisar `fields_extracted`** → verificar qué se extrajo correctamente
3. **Esperar ~5 min** → GET /webhook/{ingestion_id} → confirmar `"stored"`
4. **Listar** → GET /webhook?date=2026-08-04 → ver todos los del día

---

## Preservación de Datos

**Nada se pierde.** El JSON completo se guarda en `raw_event_json`. Los campos reconocidos se extraen a columnas indexadas. Campos nuevos o desconocidos quedan en raw_event_json.

---

## Consideraciones

- Solo objetos JSON (`{}`). Arrays rechazados con 400.
- UTF-8 soportado (emojis, acentos, ñ)
- No hay validación de duplicados
- Latencia: POST inmediato, datos en Parquet en ~5 min
- GET con date obligatorio para listar

---

## Contacto

Para solicitar token de acceso o reportar problemas: equipo de infraestructura.
