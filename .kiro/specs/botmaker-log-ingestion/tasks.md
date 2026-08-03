# Implementation Plan: Botmaker Log Ingestion Pipeline

## Overview

This plan implements a serverless log ingestion pipeline that receives Botmaker webhook events via API Gateway HTTP API, normalizes them into a 16-field schema using a Python 3.12 Lambda, converts to Parquet via Amazon Data Firehose, and stores in S3 with daily partitions queryable via Athena. The implementation includes full CI/CD automation with CodePipeline (8 stages) and comprehensive testing (unit, property-based, schema, infrastructure, integration, and smoke tests).

## Tasks

- [ ] 1. Set up project structure and core configuration
  - [x] 1.1 Create project directory structure and configuration files
    - Create `src/` directory with `__init__.py`
    - Create `tests/unit/`, `tests/properties/`, `tests/schema/`, `tests/infra/`, `tests/integration/`, `tests/smoke/` directories with `__init__.py`
    - Create `requirements.txt` with boto3, aws-lambda-powertools
    - Create `requirements-dev.txt` with pytest, pytest-cov, hypothesis, pyarrow, cfn-lint, aws-sam-cli, moto
    - Create `samconfig.toml` with default deployment configuration
    - _Requirements: 10.3, 11.3, 15.8_

  - [x] 1.2 Implement `src/config.py` — environment-based configuration module
    - Define configuration class reading from environment variables: ENVIRONMENT, FIREHOSE_STREAM_NAME, SECRET_ID, LOG_LEVEL
    - Include constants: SCHEMA_VERSION = 1, MAX_BODY_SIZE_BYTES = 1_048_576 (1 MB), SECRET_CACHE_TTL = 300, SECRET_TIMEOUT = 5
    - _Requirements: 2.4, 4.4, 16.1, 16.5_

  - [x] 1.3 Implement `src/logger.py` — structured JSON logging with redaction
    - Create structured logger emitting JSON with: timestamp (ISO-8601), level, correlation_id, event_type, outcome, duration_ms
    - Implement log redaction that recursively removes sensitive values (tokens, secrets, API keys) from log payloads
    - Support event types: INGESTION_RECEIVED, INGESTION_VALIDATED, FIREHOSE_ACCEPTED
    - Ensure raw request bodies are never logged; only metadata fields
    - _Requirements: 9.2, 9.4, 9.5, 16.2_

- [x] 2. Implement authentication module
  - [x] 2.1 Implement `src/auth.py` — Bearer token validation with secret caching
    - Implement `SecretCache` class with in-memory TTL-based caching (5-minute TTL)
    - Implement `validate_token(event, secret_cache, sm_client)` that extracts Bearer token from Authorization header
    - Return HTTP 401 with safe error message for missing/invalid/malformed tokens
    - Handle Secrets Manager timeout (>5 seconds) by aborting and returning failure without exposing secret name
    - Log rejection events including source IP and timestamp without exposing credential values
    - _Requirements: 1.3, 16.1, 16.5, 9.4_

  - [x]* 2.2 Write unit tests for `src/auth.py`
    - Test valid token authentication succeeds
    - Test invalid token returns 401
    - Test missing Authorization header returns 401
    - Test malformed header (no "Bearer " prefix) returns 401
    - Test Secrets Manager errors (timeout, access denied)
    - Test secret caching (cache hit, cache expiry, re-fetch)
    - Test that response bodies do not contain credential values
    - _Requirements: 10.2_

  - [x]* 2.3 Write property test for invalid authentication rejection
    - **Property 7: Invalid Authentication Rejection**
    - Generate arbitrary strings ≠ configured secret, verify 401 response
    - Verify response body never contains the expected secret value
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 1.3**

- [x] 3. Implement normalization module
  - [x] 3.1 Implement `src/normalizer.py` — field extraction and schema normalization
    - Implement `normalize(body: dict, request_id: str, environment: str) -> dict` producing 16-field Normalized_Record
    - Implement `get_nested(body, *keys)` for safe dot-notation traversal (returns None for absent/non-dict intermediates)
    - Implement `extract_event_id(body)` with fallback chain: eventId → errorId → linkId → messageId → conversation.messageId → UUID v4
    - Implement `extract_event_type(body)` with rule chain: eventType → error.type → type → PAYMENT_LINK_EVENT → ORDER_EVENT → UNKNOWN
    - Implement extraction for: conversation_id, message_id, user_id, order_form_id, source_channel, source_bot, severity, error_code
    - Treat null and empty strings as absent in all fallback chains
    - Generate UUID v4 for ingestion_id, ISO-8601 UTC timestamp with ms precision for received_at
    - Set schema_version to 1, store raw_event_json using json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    - Validate required source fields (event_type derivable from body); reject if event_type resolves only to UNKNOWN when no eventType/error.type/type/paymentLink/orders exist AND environment is absent
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12, 3.13, 4.2_

  - [x]* 3.2 Write unit tests for `src/normalizer.py`
    - Test all 16 fields produced with correct snake_case names
    - Test each extraction chain individually with specific fixtures
    - Test get_nested with absent keys, null intermediates, non-dict intermediates, empty strings
    - Test UUID v4 generation for ingestion_id
    - Test ISO-8601 format for received_at
    - Test schema_version is always 1
    - Test raw_event_json compact serialization with ensure_ascii=False
    - Test missing required field rejection (event_type not derivable)
    - _Requirements: 10.2_

  - [x]* 3.3 Write property tests for normalization invariant
    - **Property 1: Normalization Invariant (16-field structure)**
    - Generate arbitrary webhook JSON objects using Hypothesis strategies
    - Verify exactly 16 keys in output, all snake_case
    - Verify required fields (ingestion_id, event_id, received_at, schema_version, environment, event_type, raw_event_json) are non-null
    - Verify ingestion_id is valid UUID v4, received_at is ISO-8601, schema_version == 1
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6**

  - [x]* 3.4 Write property tests for fallback chain priority
    - **Property 2: Fallback Chain Priority**
    - Generate objects with multiple candidate values for event_id, event_type, message_id, order_form_id
    - Verify highest-priority non-null, non-empty-string source is selected
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 3.1, 3.2, 3.4, 3.6**

  - [x]* 3.5 Write property tests for path traversal null safety
    - **Property 3: Path Traversal Null Safety**
    - Generate objects with broken paths (null intermediates, absent keys, non-object intermediates, empty strings)
    - Verify null stored for affected fields
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 3.3, 3.5, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12, 3.13**

  - [x]* 3.6 Write property test for raw event JSON round-trip
    - **Property 4: Raw Event JSON Round-Trip**
    - Generate arbitrary webhook JSON objects
    - Verify json.loads(normalized_record["raw_event_json"]) == original_body
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 4.2**

  - [x]* 3.7 Write property test for missing required field rejection
    - **Property 8: Missing Required Field Rejection**
    - Generate JSON objects where event_type cannot be derived from any fallback
    - Verify rejection with error identifying the missing field
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 2.7**

- [x] 4. Implement Firehose client and handler
  - [x] 4.1 Implement `src/firehose_client.py` — PutRecord wrapper with error handling
    - Implement `deliver_record(record: dict, stream_name: str, client) -> str` that serializes record as single-line JSON UTF-8 and calls PutRecord
    - Handle SDK retry exhaustion by raising a specific exception that maps to HTTP 503
    - Use boto3 adaptive retry mode (max 3 attempts)
    - _Requirements: 5.1, 5.12, 5.13_

  - [x] 4.2 Implement `src/handler.py` — Lambda entry point and request routing
    - Implement `lambda_handler(event, context)` as the main entry point
    - Orchestrate the request flow: auth → parse → validate → normalize → deliver → respond
    - Handle base64-encoded bodies (isBase64Encoded flag)
    - Validate JSON parsing, reject non-JSON with HTTP 400
    - Validate body is a JSON object (not array/primitive), reject with HTTP 400
    - Validate decoded body size ≤ 1 MB, reject with HTTP 400
    - Validate raw payload size ≤ 6 MB, reject with HTTP 413
    - Return HTTP 202 Accepted only after successful PutRecord with ingestion_id in response
    - Return HTTP 503 Service Unavailable if PutRecord fails after retries
    - Generate correlation_id from API Gateway request ID for all log entries
    - Log INGESTION_RECEIVED, INGESTION_VALIDATED, FIREHOSE_ACCEPTED at appropriate stages
    - _Requirements: 1.2, 1.4, 1.5, 4.1, 4.3, 4.4, 5.12, 5.13, 9.2, 9.5_

  - [x]* 4.3 Write unit tests for `src/firehose_client.py`
    - Test successful PutRecord returns record ID
    - Test Firehose unavailable raises appropriate exception
    - Test timeout raises appropriate exception
    - Test permission errors
    - Test SDK retry behavior
    - _Requirements: 10.2_

  - [x]* 4.4 Write unit tests for `src/handler.py`
    - Test full happy path (valid auth, valid JSON, normalize, deliver, 202 response)
    - Test invalid JSON body returns 400
    - Test array body returns 400
    - Test primitive body (string, number, boolean, null) returns 400
    - Test base64-encoded body decoding
    - Test oversized payload (>6 MB) returns 413
    - Test oversized decoded body (>1 MB) returns 400
    - Test empty body returns 400
    - Test Unicode handling
    - Test Firehose failure returns 503
    - Test correlation ID propagation across all log entries
    - _Requirements: 10.2_

  - [x]* 4.5 Write property test for base64 decoding equivalence
    - **Property 5: Base64 Decoding Equivalence**
    - Generate valid webhook JSON, process with isBase64Encoded=True (base64 body) and False (raw body)
    - Verify identical Normalized_Record (excluding received_at and ingestion_id)
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 4.1**

  - [x]* 4.6 Write property test for non-object JSON rejection
    - **Property 6: Non-Object JSON Rejection**
    - Generate non-object JSON values (arrays, strings, numbers, booleans, null)
    - Verify HTTP 400 response and no Normalized_Record produced
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 4.3**

  - [x]* 4.7 Write property test for log redaction safety
    - **Property 9: Log Redaction Safety**
    - Generate requests containing authentication tokens/secrets in various positions
    - Verify no log entry contains the literal token value
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 9.4, 16.2**

- [x] 5. Checkpoint - Core Lambda implementation
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Create SAM/CloudFormation infrastructure template — core resources
  - [x] 6.1 Create `template.yaml` with Parameters, Globals, and S3 Bucket resource
    - Define all CloudFormation parameters: FirehoseBufferIntervalSeconds (300, min 60, max 900), FirehoseBufferSizeMB (64, min 64, max 128), PartitionProjectionStartDate (2026-01-01), ParquetCompression (SNAPPY), AthenaBytesScannedCutoffPerQuery, AthenaResultsRetentionDays (30), EnableAthenaWorkGroup (true/false)
    - Define S3 Bucket with: SSE encryption, PublicAccessBlockConfiguration (all 4 true), BucketPolicy denying non-TLS (aws:SecureTransport=false)
    - Define CloudFormation Condition for EnableAthenaWorkGroup
    - _Requirements: 17.1, 17.2, 17.3, 17.4, 17.5, 17.6, 17.7, 16.4, 12.3, 12.4, 12.5_

  - [x] 6.2 Add Lambda function, API Gateway HTTP API, and Secrets Manager resources to `template.yaml`
    - Define Lambda function: Python 3.12, arm64, 256 MB memory, 10s timeout, 1000 reserved concurrency
    - Define API Gateway HTTP API with POST /webhook route and Lambda proxy integration (payload format 2.0)
    - Enable API Gateway access logging with 30-day retention
    - Define Lambda IAM role with least-privilege: firehose:PutRecord (stream ARN), secretsmanager:GetSecretValue (secret ARN), CloudWatch Logs (log group ARN)
    - Ensure NO wildcard actions or resources, NO s3:Put*/DeleteObject/CopyObject on Lambda role
    - _Requirements: 1.1, 1.2, 1.6, 1.7, 8.2, 8.3, 8.4, 8.5, 9.1_

  - [x] 6.3 Add Firehose delivery stream resource to `template.yaml`
    - Define Firehose with DirectPut source, Extended S3 destination
    - Configure DataFormatConversionConfiguration: OpenX JSON SerDe input, Parquet output with configurable compression
    - Set S3 prefixes: data/ingestion_date=!{timestamp:yyyy-MM-dd}/ (success), firehose-errors/type=!{firehose:error-output-type}/ingestion_date=!{timestamp:yyyy-MM-dd}/ (errors)
    - Configure buffer: SizeInMBs from parameter, IntervalInSeconds from parameter
    - Set CompressionFormat to UNCOMPRESSED (Parquet handles compression internally)
    - Set .parquet extension, server-side encryption, CloudWatch error logging
    - Configure custom timezone America/Bogota
    - Define Firehose IAM role with least-privilege: s3:PutObject/PutObjectAcl (bucket ARN/*), glue:GetTable/GetTableVersions (table ARN), kms:Decrypt/GenerateDataKey (key ARN), CloudWatch Logs
    - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 5.10, 5.11, 8.1, 8.3, 8.5_

  - [x] 6.4 Add Glue Data Catalog resources to `template.yaml`
    - Define Glue Database with name pattern {project_name}_{environment}
    - Define Glue Table (botmaker_logs) with all 16 columns + ingestion_date partition key
    - Set SerDe to ParquetHiveSerDe with MapredParquet input/output formats, classification=parquet
    - Set table location to s3://{bucket}/data/
    - Enable partition projection: projection.enabled=true, ingestion_date type=date, format=yyyy-MM-dd, interval=1 DAY, range from PartitionProjectionStartDate to NOW
    - Set storage.location.template to s3://{bucket}/data/ingestion_date=${ingestion_date}/
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8_

  - [x] 6.5 Add Athena WorkGroup and named queries to `template.yaml` (conditional)
    - Define Athena WorkGroup (conditional on EnableAthenaWorkGroup): results in s3://{bucket}/athena-results/, SSE encryption, CloudWatch metrics, enforced settings
    - Add BytesScannedCutoffPerQuery from parameter (conditional)
    - Define at least 4 named queries: record count by date, recent events, events by type, error events by severity
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.6, 7.7, 7.8_

  - [x] 6.6 Add CloudFormation Outputs to `template.yaml`
    - Output: GlueDatabaseName, GlueTableName, GlueTableArn, AthenaWorkGroupName (conditional), AthenaResultsLocation (conditional), ParquetDataPrefix, FirehoseErrorPrefix, ApiEndpoint
    - Use Conditions to omit Athena outputs when EnableAthenaWorkGroup=false
    - _Requirements: 7.5, 18.1, 18.3_

- [x] 7. Implement infrastructure and schema validation tests
  - [x] 7.1 Create `tests/infra/test_template_validation.py` — SAM and cfn-lint validation
    - Test sam validate --lint passes for template.yaml
    - Test cfn-lint passes for template.yaml
    - Verify S3 PublicAccessBlockConfiguration has all 4 settings true
    - Verify S3 and Firehose encryption enabled
    - Verify S3 bucket policy denies non-TLS requests
    - Verify no wildcard IAM actions or resources with allow effect
    - Verify Firehose DataFormatConversionConfiguration enabled
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7_

  - [x] 7.2 Create `tests/schema/test_parquet_schema.py` — Parquet schema round-trip tests
    - Define PyArrow schema matching Glue table (16 fields + ingestion_date)
    - Validate each field type maps correctly to Parquet column type from Glue definition
    - Write Parquet file locally, read back, assert schema match, row count, and field values
    - Test with all-null optional fields record
    - Test with all-populated fields record
    - Fail with assertion identifying mismatched field and expected vs actual type
    - _Requirements: 11.1, 11.2, 11.3, 11.5_

  - [x]* 7.3 Write property test for Parquet schema round-trip
    - **Property 10: Parquet Schema Round-Trip**
    - Generate valid Normalized_Records via Hypothesis
    - Write to Parquet using Glue schema, read back, verify field-by-field equality including nulls
    - Minimum 100 Hypothesis examples
    - **Validates: Requirements 11.4**

- [x] 8. Checkpoint - Infrastructure and schema tests
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Create CI/CD Pipeline CloudFormation stack
  - [x] 9.1 Create `pipeline/codepipeline.yaml` with pipeline parameters and source stage
    - Define parameters: ConnectionArn, RepositoryOwner, RepositoryName, BranchName (default main), DevStackName, ProdStackName, ApprovalNotificationArn
    - Define Source stage using AWS CodeConnections (no personal access tokens)
    - Define SNS topic or reference configurable ARN for notifications
    - _Requirements: 15.1, 15.2, 15.9, 16.3_

  - [x] 9.2 Add build, deploy, test, and approval stages to `pipeline/codepipeline.yaml`
    - Define 8 stages in order: Source, ValidateAndTest, Package, DeployDev, IntegrationTests, ManualApproval, DeployProd, SmokeTestProd
    - Configure ManualApproval with 7-day timeout and SNS notification
    - Configure stage transitions: integration test failure stops pipeline
    - Define CodeBuild projects referencing separate buildspec files
    - Configure CodeBuild report groups for JUnit XML and Coverage XML
    - Add CI_CD_Pipeline outputs: PipelineName, DevStackName, ProdStackName
    - _Requirements: 15.3, 15.4, 15.5, 15.6, 15.7, 15.8, 15.10, 15.11, 18.2_

  - [x] 9.3 Create buildspec files for all CI/CD stages
    - Create `buildspec-ci.yml`: install deps, run sam validate --lint, cfn-lint, pytest (unit + property + schema + infra), publish reports
    - Create `buildspec-package.yml`: sam build, sam package, output artifacts
    - Create `buildspec-integration.yml`: install deps, run pytest tests/integration/ against dev stack, publish reports
    - Create `buildspec-smoke.yml`: install deps, run pytest tests/smoke/ against prod stack, publish reports
    - _Requirements: 15.7, 15.8_

- [x] 10. Implement integration and smoke test suites
  - [x] 10.1 Create `tests/integration/test_pipeline_e2e.py` — end-to-end integration tests
    - Retrieve API endpoint from CloudFormation stack outputs
    - Test HTTP 202 with valid auth and valid payload (generate unique ingestion_id per run)
    - Test HTTP 401 with invalid/missing token
    - Test HTTP 400 with invalid JSON and non-object JSON
    - Test HTTP 413 with oversized payload
    - Test HTTP 404/405 for undefined routes and methods
    - Poll CloudWatch Logs (60s) for INGESTION_RECEIVED, INGESTION_VALIDATED, FIREHOSE_ACCEPTED
    - Wait for Firehose buffer flush (330s), download Parquet from S3, validate 16 columns and field values
    - Execute Athena query filtering by unique ingestion_id, verify single row returned with correct values (60s timeout)
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_

  - [x] 10.2 Create `tests/smoke/test_smoke.py` — production smoke test suite
    - Submit event with event_type=PIPELINE_SMOKE_TEST, verify HTTP 202
    - Poll CloudWatch Logs for FIREHOSE_ACCEPTED within 30 seconds
    - Verify event_type=PIPELINE_SMOKE_TEST preserved in pipeline
    - Report pass/fail with identification of which step failed if timeout
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5_

- [x] 11. Create documentation
  - [x] 11.1 Create README.md with setup, deployment, configuration, and query documentation
    - Document AWS CodeConnections GitHub setup process (create connection, authorize, confirm Available, retrieve ARN)
    - Document initial pipeline deployment procedure (prerequisites: AWS CLI, SAM CLI, Python; deploy command with parameters)
    - List all configurable CloudFormation parameters with names, descriptions, default values
    - Include 4+ ready-to-copy Athena query examples: filter by ingestion_date range, filter by event_type, json_extract_scalar from raw_event_json, exclude PIPELINE_SMOKE_TEST
    - _Requirements: 19.1, 19.2, 19.3, 19.4_

- [x] 12. Final checkpoint - Complete implementation
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- Python 3.12 is used for all Lambda code and tests
- Hypothesis (minimum 100 examples) is used for all property-based tests
- PyArrow is a dev-only dependency (not included in Lambda runtime)
- Infrastructure tests use cfn-lint and sam validate (no live AWS resources needed)
- Integration tests require a deployed dev stack; smoke tests require a deployed prod stack

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.3"] },
    { "id": 2, "tasks": ["2.1", "3.1", "4.1"] },
    { "id": 3, "tasks": ["2.2", "2.3", "3.2", "3.3", "3.4", "3.5", "3.6", "3.7", "4.3"] },
    { "id": 4, "tasks": ["4.2"] },
    { "id": 5, "tasks": ["4.4", "4.5", "4.6", "4.7"] },
    { "id": 6, "tasks": ["6.1"] },
    { "id": 7, "tasks": ["6.2", "6.3", "6.4"] },
    { "id": 8, "tasks": ["6.5", "6.6"] },
    { "id": 9, "tasks": ["7.1", "7.2"] },
    { "id": 10, "tasks": ["7.3"] },
    { "id": 11, "tasks": ["9.1"] },
    { "id": 12, "tasks": ["9.2"] },
    { "id": 13, "tasks": ["9.3"] },
    { "id": 14, "tasks": ["10.1", "10.2"] },
    { "id": 15, "tasks": ["11.1"] }
  ]
}
```
