# Contrato de Integración — Botmaker Log Ingestion

## Resumen

Este servicio recibe eventos webhook de Botmaker mediante HTTP POST, los normaliza y almacena en formato Parquet para consulta analítica. Cualquier JSON object válido será aceptado y almacenado sin pérdida de información.

---

## Endpoint

| Atributo | Valor |
|----------|-------|
| URL | `https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook` |
| Método | `POST` |
| Protocolo | HTTPS (TLS 1.2+) |
| Content-Type | `application/json` |
| Autenticación | Bearer Token |

---

## Autenticación

Todas las peticiones deben incluir el header `Authorization` con un Bearer token válido.

```
Authorization: Bearer <token>
```

El token es proporcionado por el equipo de infraestructura. Sin este header, la petición será rechazada con HTTP 401.

---

## Formato del Body

El body debe ser un **objeto JSON válido** (`{...}`). No se aceptan arrays, strings, números ni valores null como body.

### Restricciones

| Restricción | Límite |
|-------------|--------|
| Tamaño máximo del body | 6 MB |
| Tamaño máximo decodificado | 1 MB |
| Formato | JSON Object (`{...}`) |
| Encoding | UTF-8 |
| Base64 | Soportado (API Gateway puede codificar en base64) |

### Estructura libre

El servicio acepta **cualquier estructura JSON**. No hay un esquema obligatorio. Sin embargo, los siguientes campos son reconocidos y se extraen automáticamente a columnas optimizadas para consulta:

---

## Campos Reconocidos

### Identificador del evento (`event_id`)

Se extrae del primer campo no vacío en este orden de prioridad:

1. `eventId`
2. `errorId`
3. `linkId`
4. `messageId`
5. `conversation.messageId`
6. UUID generado automáticamente (si ninguno existe)

### Tipo de evento (`event_type`)

Se extrae del primer campo/condición que aplique:

1. `eventType`
2. `error.type`
3. `type`
4. Si existe la key `paymentLink` → `PAYMENT_LINK_EVENT`
5. Si existe la key `orders` → `ORDER_EVENT`
6. `UNKNOWN` (si nada aplica)

### Otros campos extraídos

| Columna | Fuente en el JSON | Ejemplo |
|---------|-------------------|---------|
| `conversation_id` | `conversation.conversationId` | `"conv_123456"` |
| `message_id` | `messageId` o `conversation.messageId` | `"wamid.xxx"` |
| `user_id` | `conversation.userId` | `"user_987654"` |
| `order_form_id` | `orderForm.id` o `context.orderFormId` | `"abc123"` |
| `source_channel` | `source.channel` | `"whatsapp"` |
| `source_bot` | `source.bot` | `"agentcore"` |
| `severity` | `error.severity` | `"ERROR"` |
| `error_code` | `error.code` | `"CATALOG_001"` |

Si un campo no existe en el JSON enviado, se almacena como `null`.

---

## Ejemplo de Petición — Evento de Error

```bash
curl -X POST https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "errorId": "err_01JXYZ123",
    "timestamp": "2026-07-28T17:35:42.123Z",
    "source": {
      "channel": "whatsapp",
      "bot": "agentcore"
    },
    "conversation": {
      "conversationId": "conv_123456",
      "userId": "user_987654",
      "phone": "********1234",
      "messageId": "wamid.xxx"
    },
    "error": {
      "type": "EXTERNAL_API_ERROR",
      "code": "CATALOG_001",
      "message": "Unable to retrieve product availability",
      "severity": "ERROR"
    },
    "context": {
      "operation": "getProductAvailability",
      "orderFormId": "abc123"
    },
    "userMessage": {
      "sent": true,
      "message": "Lo sentimos, no pudimos procesar tu solicitud."
    }
  }'
```

**Respuesta exitosa:**
```json
HTTP 202 Accepted

{"status": "accepted", "ingestion_id": "48ae50a0-28af-46a3-9af5-ce3c0c490ff6"}
```

---

## Ejemplo de Petición — Evento de Link de Pago

```bash
curl -X POST https://5g907s87s5.execute-api.us-east-1.amazonaws.com/webhook \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "linkId": "plink_01JXYZ123",
    "store": {
      "account": "tottoco",
      "domain": "https://co.totto.com"
    },
    "orderForm": {
      "id": "abc123"
    },
    "paymentLink": {
      "status": "ACTIVE",
      "expiresAt": "2026-07-28T17:35:42.123Z"
    },
    "vtex_customer": {
      "id": "customer_123"
    },
    "conversation": {
      "conversationId": "conv_123456",
      "userId": "user_987654",
      "phone": "********1234",
      "messageId": "wamid.xxx"
    },
    "orders": [
      {
        "orderId": "243468274-01",
        "itemsCount": 3,
        "total": 25000000,
        "payment": {
          "status": "APPROVED",
          "paidAt": "2026-07-28T17:35:42.123Z"
        }
      }
    ],
    "source": {
      "channel": "whatsapp",
      "bot": "agentcore"
    },
    "createdAt": "2026-07-28T17:35:42.123Z",
    "updatedAt": "2026-07-28T17:35:42.123Z"
  }'
```

**Respuesta exitosa:**
```json
HTTP 202 Accepted

{"status": "accepted", "ingestion_id": "7f2d1a3b-4c5e-6f7a-8b9c-0d1e2f3a4b5c"}
```

---

## Códigos de Respuesta

| Código | Significado | Cuándo ocurre |
|--------|-------------|---------------|
| `202 Accepted` | Evento recibido y procesado | Petición válida con autenticación correcta |
| `400 Bad Request` | Body inválido | JSON mal formado, body es un array, o excede 1 MB |
| `401 Unauthorized` | Token inválido | Header Authorization ausente, malformado o con token incorrecto |
| `413 Payload Too Large` | Body demasiado grande | Body excede 6 MB |
| `404 Not Found` | Ruta no existe | URL diferente a `/webhook` |
| `405 Method Not Allowed` | Método no soportado | Método diferente a POST (GET, PUT, etc.) |
| `503 Service Unavailable` | Error interno temporal | El servicio de almacenamiento no respondió; reintentar |

---

## Reintentos

Si recibes HTTP `503`, el servicio tuvo un problema temporal. Recomendaciones:

- Reintentar con backoff exponencial (1s, 2s, 4s, 8s...)
- Máximo 3-5 reintentos
- No reintentar para 400, 401, 413 (estos errores no se resuelven reintentando)

---

## Preservación de Datos

**Ningún dato se pierde.** El JSON completo enviado se almacena tal cual en una columna llamada `raw_event_json`. Los campos reconocidos (tabla anterior) se extraen adicionalmente a columnas indexadas para consultas rápidas.

### Lo que se extrae a columnas (consulta rápida)
- event_id, event_type, conversation_id, message_id, user_id, order_form_id, source_channel, source_bot, severity, error_code

### Lo que queda en raw_event_json (consulta con json_extract)
- **Todo.** Incluyendo: timestamp, error.message, context.operation, userMessage, store, paymentLink, orders, vtex_customer, phone, createdAt, updatedAt, y cualquier otro campo presente o futuro.

---

## Campos Generados Automáticamente

Estos campos se agregan por el servicio y no necesitan ser enviados:

| Campo | Descripción |
|-------|-------------|
| `ingestion_id` | UUID único por evento (para tracking) |
| `request_id` | ID de la petición HTTP (correlación con logs) |
| `received_at` | Timestamp UTC del momento de recepción |
| `schema_version` | Versión del esquema de normalización (actualmente `1`) |
| `environment` | Ambiente donde se procesó (`dev` / `prod`) |
| `ingestion_date` | Fecha de llegada en timezone America/Bogota (partición diaria) |

---

## Consideraciones

1. **No enviar arrays como body** — Solo objetos JSON (`{}`). Un array `[...]` será rechazado con 400.
2. **Encoding UTF-8** — Caracteres Unicode (emojis, acentos, ñ) son soportados.
3. **Campos opcionales** — Todos los campos son opcionales. Envía lo que tengas disponible.
4. **Idempotencia** — El servicio NO valida duplicados. Si envías el mismo evento dos veces, se almacena dos veces con diferentes `ingestion_id`.
5. **Orden** — No se garantiza orden de procesamiento. Cada evento es independiente.
6. **Latencia** — La respuesta 202 llega en menos de 3 segundos. Los datos están disponibles en Athena después de ~5 minutos (buffer de Firehose).

---

## Contacto

Para solicitar el token de acceso o reportar problemas con el servicio, contactar al equipo de ciencia de datos.
