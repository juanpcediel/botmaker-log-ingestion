# Requirements Document

## Introduction

This document defines the requirements for a serverless log ingestion pipeline that receives webhook events from Botmaker, normalizes them into a stable schema, converts records to Apache Parquet via Amazon Data Firehose, stores them in S3 with daily partitions, and makes them queryable via Amazon Athena. The system includes full CI/CD automation with AWS CodePipeline and CodeBuild.

## Glossary

- **Ingestion_Pipeline**: The complete serverless system comprising API Gateway, Lambda, Firehose, S3, Glue Data Catalog, and Athena that processes Botmaker webhook events end-to-end.
- **Ingestion_Lambda**: The AWS Lambda function that receives webhook events, authenticates requests, normalizes payloads, and sends records to Firehose via PutRecord.
- **Firehose_Stream**: The Amazon Data Firehose delivery stream configured for Direct PUT that converts JSON records to Parquet and delivers them to S3.
- **Glue_Catalog**: The AWS Glue Data Catalog database and table that defines the Parquet schema and enables partition projection for Athena queries.
- **Athena_WorkGroup**: The Amazon Athena WorkGroup configured with encryption, metrics, and enforced settings for querying ingested logs.
- **Normalized_Record**: A flat JSON object with exactly 16 fields in snake_case that the Ingestion_Lambda produces from each webhook event before sending to Firehose.
- **CI_CD_Pipeline**: The AWS CodePipeline that automates source retrieval, validation, testing, packaging, deployment to dev, integration testing, manual approval, and production deployment.
- **Partition_Projection**: An Athena feature that uses table properties to infer partition existence without requiring a Glue Crawler or MSCK REPAIR TABLE.
- **Raw_Event_JSON**: The original webhook JSON body preserved as a string field within the Normalized_Record for future ad-hoc querying.
- **CodeBuild_Project**: An AWS CodeBuild project that executes buildspec files for CI validation, packaging, integration tests, or smoke tests.

## Requirements

### Requirement 1: Webhook Event Reception

**User Story:** As a data engineer, I want the pipeline to receive Botmaker webhook events via an HTTP endpoint, so that external events are reliably captured for processing.

#### Acceptance Criteria

1. WHEN a POST request is received at the ingestion endpoint, THE Ingestion_Pipeline SHALL route the request to the Ingestion_Lambda via API Gateway HTTP API.
2. WHEN a request is received with a valid authentication token matching the configured webhook secret, THE Ingestion_Lambda SHALL return HTTP 202 Accepted to acknowledge receipt within 3 seconds of request arrival.
3. IF a request is received without a valid authentication token, THEN THE Ingestion_Lambda SHALL return HTTP 401 Unauthorized and log the rejection event including the source IP and timestamp, without exposing credentials or token values in the response body.
4. IF the request body is not valid JSON, THEN THE Ingestion_Lambda SHALL return HTTP 400 Bad Request with an error message indicating that the payload is not parseable JSON.
5. IF the request body exceeds 6 MB, THEN THE Ingestion_Lambda SHALL return HTTP 413 Payload Too Large without processing the payload.
6. IF a request is sent to an undefined route or unsupported method, THEN THE Ingestion_Pipeline SHALL return HTTP 404 Not Found or 405 Method Not Allowed respectively.
7. IF the Ingestion_Lambda receives more than 1000 concurrent requests, THEN THE Ingestion_Pipeline SHALL return HTTP 429 Too Many Requests for excess requests and process queued requests in arrival order.

### Requirement 2: Event Normalization

**User Story:** As a data engineer, I want webhook events normalized into a stable schema, so that downstream queries operate on consistent, predictable columns.

#### Acceptance Criteria

1. WHEN a webhook event that passes validation is received, THE Ingestion_Lambda SHALL produce a Normalized_Record with exactly these 16 fields: ingestion_id, event_id, request_id, received_at, schema_version, environment, event_type, conversation_id, message_id, user_id, order_form_id, source_channel, source_bot, severity, error_code, raw_event_json.
2. THE Ingestion_Lambda SHALL use snake_case for all field names in the Normalized_Record.
3. THE Ingestion_Lambda SHALL set the following fields as required (must be non-null in every Normalized_Record): ingestion_id, event_id, received_at, schema_version, environment, event_type, raw_event_json; and SHALL set all remaining fields (request_id, conversation_id, message_id, user_id, order_form_id, source_channel, source_bot, severity, error_code) to null when the corresponding value is absent from the source event.
4. THE Ingestion_Lambda SHALL set schema_version to the integer value 1 for all records produced under this specification.
5. THE Ingestion_Lambda SHALL set received_at to the current timestamp in ISO-8601 UTC format with millisecond precision (e.g., "2024-01-15T09:30:00.123Z") at the moment of processing.
6. THE Ingestion_Lambda SHALL set ingestion_id to a newly generated UUID v4 value for each processed event.
7. IF a validated webhook event is missing any of the required source fields (event_id, environment, event_type), THEN THE Ingestion_Lambda SHALL reject the event without producing a Normalized_Record and SHALL return an error indication specifying which required field is missing.

### Requirement 3: Field Extraction Rules

**User Story:** As a data engineer, I want common fields extracted from varied webhook payloads using defined fallback chains, so that queryable columns are populated consistently regardless of event structure.

#### Acceptance Criteria

1. WHEN extracting event_id, THE Ingestion_Lambda SHALL use the first non-null and non-empty-string value from the chain: eventId, errorId, linkId, messageId, conversation.messageId, then generate a v4 UUID if all are absent.
2. WHEN extracting event_type, THE Ingestion_Lambda SHALL use the first matching rule: eventType field, error.type field, type field, PAYMENT_LINK_EVENT if paymentLink key exists, ORDER_EVENT if orders key exists, then UNKNOWN if none match.
3. WHEN extracting conversation_id, THE Ingestion_Lambda SHALL read the value from conversation.conversationId.
4. WHEN extracting message_id, THE Ingestion_Lambda SHALL use the first non-null and non-empty-string value from: messageId, conversation.messageId.
5. WHEN extracting user_id, THE Ingestion_Lambda SHALL read the value from conversation.userId.
6. WHEN extracting order_form_id, THE Ingestion_Lambda SHALL use the first non-null and non-empty-string value from: orderForm.id, context.orderFormId.
7. WHEN extracting source_channel, THE Ingestion_Lambda SHALL read the value from source.channel.
8. WHEN extracting source_bot, THE Ingestion_Lambda SHALL read the value from source.bot.
9. WHEN extracting severity, THE Ingestion_Lambda SHALL read the value from error.severity.
10. WHEN extracting error_code, THE Ingestion_Lambda SHALL read the value from error.code.
11. IF a single-source field path (conversation_id, user_id, source_channel, source_bot, severity, or error_code) resolves to null, is an empty string, or any intermediate key in the path is absent, THEN THE Ingestion_Lambda SHALL store null for that column.
12. WHEN traversing a dot-notation field path, IF an intermediate key is absent or its value is not an object, THEN THE Ingestion_Lambda SHALL treat the full path as absent and proceed to the next fallback in the chain or store null if no fallback exists.
13. WHEN evaluating fallback chains, THE Ingestion_Lambda SHALL treat both null values and empty strings as absent and skip to the next candidate in the chain.

### Requirement 4: JSON Preservation

**User Story:** As a data analyst, I want the complete original JSON preserved in each record, so that I can query any field not extracted into top-level columns using json_extract_scalar.

#### Acceptance Criteria

1. WHEN the incoming request has the isBase64Encoded flag set to true, THE Ingestion_Lambda SHALL decode the body from base64 before further processing.
2. THE Ingestion_Lambda SHALL store the complete original JSON object as a string in the raw_event_json field using json.dumps with ensure_ascii=False and separators=(",", ":").
3. IF the decoded body is not a valid JSON object (top-level value is not a JSON object delimited by curly braces), THEN THE Ingestion_Lambda SHALL reject the request with HTTP 400 Bad Request and include an error message indicating the body is not a valid JSON object.
4. IF the decoded JSON body exceeds 1 MB in size, THEN THE Ingestion_Lambda SHALL reject the request with HTTP 400 Bad Request and include an error message indicating the payload exceeds the maximum allowed size.

### Requirement 5: Firehose Delivery and Parquet Conversion

**User Story:** As a data engineer, I want records converted to Parquet and stored in S3 with daily partitions, so that query performance and storage costs are optimized.

#### Acceptance Criteria

1. WHEN the Ingestion_Lambda has produced a Normalized_Record, THE Ingestion_Lambda SHALL serialize the record as a single-line JSON UTF-8 string and send it to the Firehose_Stream using the PutRecord API.
2. THE Firehose_Stream SHALL convert incoming JSON records to Apache Parquet format using the Glue_Catalog table schema as the conversion definition.
3. THE Firehose_Stream SHALL use OpenX JSON SerDe as the input deserializer and Apache Parquet as the output serializer with Snappy compression.
4. THE Firehose_Stream SHALL set CompressionFormat to UNCOMPRESSED because compression is managed internally by the Parquet serializer.
5. THE Firehose_Stream SHALL write converted files with the .parquet extension.
6. THE Firehose_Stream SHALL write success records to the S3 prefix data/ingestion_date=!{timestamp:yyyy-MM-dd}/ using the America/Bogota custom time zone.
7. THE Firehose_Stream SHALL write conversion error records to the S3 prefix firehose-errors/type=!{firehose:error-output-type}/ingestion_date=!{timestamp:yyyy-MM-dd}/.
8. THE Firehose_Stream SHALL buffer records with configurable SizeInMBs (default 64, minimum 64 when format conversion is enabled) and IntervalInSeconds (default 300).
9. THE Firehose_Stream SHALL use DirectPut as the delivery stream type with Extended S3 destination and no Lambda transformation.
10. THE Firehose_Stream SHALL enable server-side encryption.
11. THE Firehose_Stream SHALL enable CloudWatch error logging for delivery and format conversion errors.
12. IF PutRecord fails after SDK retries are exhausted, THEN THE Ingestion_Lambda SHALL return HTTP 503 Service Unavailable to the caller.
13. THE Ingestion_Lambda SHALL return HTTP 202 Accepted only after a successful PutRecord response from the Firehose_Stream.

### Requirement 6: Glue Data Catalog Schema Definition

**User Story:** As a data engineer, I want the Parquet schema defined in the Glue Data Catalog via CloudFormation, so that Firehose can perform format conversion and Athena can query data without manual schema management.

#### Acceptance Criteria

1. THE Glue_Catalog SHALL define a database named with the pattern <project_name>_<environment>.
2. THE Glue_Catalog SHALL define a table named botmaker_logs within the database.
3. THE Glue_Catalog table SHALL define columns for all 16 fields of the Normalized_Record with the following types: ingestion_id (string), event_id (string), request_id (string), received_at (string), schema_version (int), environment (string), event_type (string), conversation_id (string), message_id (string), user_id (string), order_form_id (string), source_channel (string), source_bot (string), severity (string), error_code (string), raw_event_json (string), plus a partition key ingestion_date of type string.
4. THE Glue_Catalog table SHALL use ParquetHiveSerDe as the SerializationLibrary with MapredParquet input and output formats and set the table classification to parquet.
5. THE Glue_Catalog table SHALL set the location to s3://<logs-bucket>/data/.
6. THE Glue_Catalog table SHALL enable partition projection by setting projection.enabled to true with ingestion_date configured as type=date, format=yyyy-MM-dd, interval=1 DAY, and range from a configurable start date (default 2026-01-01) to NOW.
7. THE Glue_Catalog table SHALL define storage.location.template as s3://<logs-bucket>/data/ingestion_date=${ingestion_date}/.
8. THE Glue_Catalog database and table SHALL be provisioned via SAM/CloudFormation templates without requiring a Glue Crawler or MSCK REPAIR TABLE.

### Requirement 7: Amazon Athena Query Configuration

**User Story:** As a data analyst, I want a pre-configured Athena WorkGroup with named queries, so that I can query ingested logs immediately without manual setup.

#### Acceptance Criteria

1. THE Athena_WorkGroup SHALL store query results in s3://<logs-bucket>/athena-results/ with encryption enabled using SSE-S3 or SSE-KMS.
2. THE Athena_WorkGroup SHALL enable CloudWatch metrics publishing.
3. THE Athena_WorkGroup SHALL enforce WorkGroup settings so that individual queries cannot override result location or encryption.
4. THE Ingestion_Pipeline SHALL create at minimum these named Athena queries as CloudFormation resources: record count by date, recent events, events grouped by type, and error events filtered by severity.
5. THE Ingestion_Pipeline SHALL output AthenaWorkGroupName, GlueDatabaseName, GlueTableName, and AthenaResultsLocation as CloudFormation stack outputs.
6. WHERE the EnableAthenaWorkGroup parameter is set to true, THE Ingestion_Pipeline SHALL provision the Athena_WorkGroup, named queries, and WorkGroup configuration.
7. WHERE the AthenaBytesScannedCutoffPerQuery parameter is provided, THE Athena_WorkGroup SHALL enforce a per-query bytes-scanned limit matching that parameter value.
8. IF the EnableAthenaWorkGroup parameter is set to false, THEN THE Ingestion_Pipeline SHALL omit AthenaWorkGroupName and AthenaResultsLocation from its CloudFormation outputs.

### Requirement 8: IAM Least Privilege

**User Story:** As a security engineer, I want all IAM roles scoped to minimum necessary permissions, so that the blast radius of any compromise is minimized.

#### Acceptance Criteria

1. THE Firehose_Stream IAM role SHALL grant only the following permissions: s3:PutObject and s3:PutObjectAcl on the designated delivery bucket ARN, glue:GetTable and glue:GetTableVersions on the designated Glue table ARN, kms:Decrypt and kms:GenerateDataKey on the designated KMS key ARN, and logs:CreateLogGroup, logs:CreateLogStream, and logs:PutLogEvents on the designated CloudWatch Logs group ARN.
2. THE Ingestion_Lambda IAM role SHALL grant only the following permissions: firehose:PutRecord on the designated Firehose_Stream ARN, secretsmanager:GetSecretValue on the designated secret ARN, and logs:CreateLogGroup, logs:CreateLogStream, and logs:PutLogEvents on the designated Lambda log group ARN.
3. THE Ingestion_Pipeline SHALL NOT use wildcard characters in the Action field (such as glue:*, s3:*, kms:*, logs:*, or any other service:*) or a wildcard Resource value ("*") in any IAM policy statement.
4. THE Ingestion_Lambda SHALL NOT have any s3:Put*, s3:DeleteObject, or s3:CopyObject permissions in its IAM role policy.
5. THE Ingestion_Pipeline SHALL specify every IAM policy resource using a fully-qualified ARN with no wildcard substitution except where required by the service (such as s3:PutObject requiring the bucket ARN with a /* suffix for object-level access).

### Requirement 9: Observability and Logging

**User Story:** As an operations engineer, I want structured logs at each processing stage, so that I can trace event flow and diagnose failures.

#### Acceptance Criteria

1. THE Ingestion_Pipeline SHALL enable CloudWatch Access Logs for the API Gateway HTTP API with a minimum retention period of 30 days.
2. WHEN the Ingestion_Lambda processes an event at any stage, THE Ingestion_Lambda SHALL emit a structured JSON log entry containing at minimum: a timestamp in ISO 8601 format, a correlation ID unique to the originating request, an event type (one of INGESTION_RECEIVED, INGESTION_VALIDATED, or FIREHOSE_ACCEPTED), and a processing outcome (success or failure).
3. IF the Firehose_Stream encounters a delivery error or a format conversion error, THEN THE Firehose_Stream SHALL log the error type, the correlation ID of the affected record, and a failure reason to CloudWatch Logs.
4. THE Ingestion_Lambda SHALL NOT log secret values, authentication tokens, API keys, or full request bodies in CloudWatch Logs.
5. THE Ingestion_Lambda SHALL include the same correlation ID across all log entries for a single inbound request, enabling end-to-end tracing from INGESTION_RECEIVED through FIREHOSE_ACCEPTED within CloudWatch Logs Insights queries.

### Requirement 10: Unit Testing

**User Story:** As a developer, I want comprehensive unit tests with high coverage, so that code correctness is verified before deployment.

#### Acceptance Criteria

1. THE unit test suite SHALL achieve at least 85% code coverage of the Lambda handler source code (src/ directory) as measured by pytest-cov, excluding test files and infrastructure templates.
2. THE unit test suite SHALL cover authentication logic (valid token, invalid token, missing token, malformed header, Secrets Manager errors, secret caching), request parsing (valid JSON, invalid JSON, empty body, array body, primitive body, base64 body, invalid Content-Type, oversized payload, Unicode), normalization (all field extraction chains per Requirement 3), JSON preservation, Firehose client interaction (success, unavailable, timeout, permission errors, SDK retries), and logging redaction (no token logged, recursive redaction, LOG_PAYLOAD modes).
3. THE unit test suite SHALL use mocks for boto3 Firehose client, boto3 Secrets Manager client, Lambda context objects, and API Gateway event structures, and SHALL execute without network access.
4. THE unit test suite SHALL include property-based tests using Hypothesis (minimum 100 examples) verifying that for all valid webhook JSON objects, normalizing then serializing then deserializing the Normalized_Record produces a field-by-field equivalent Normalized_Record across all 16 fields (round-trip property).
5. THE unit test suite SHALL include property-based tests using Hypothesis (minimum 100 examples) verifying that for all valid webhook JSON objects, the Ingestion_Lambda normalization produces a record where every field in the Normalized_Record is present and no extra keys exist (invariant property).

### Requirement 11: Schema Validation Tests

**User Story:** As a developer, I want Parquet schema tests that validate normalized records match the Glue table definition, so that format conversion errors are caught before deployment.

#### Acceptance Criteria

1. THE schema test suite SHALL validate that each of the 16 Normalized_Record fields plus the ingestion_date partition key maps to the Parquet column type defined in the Glue_Catalog CloudFormation table resource, and SHALL fail with an assertion message identifying the mismatched field and expected versus actual type when a mismatch is detected.
2. THE schema test suite SHALL write at least one Parquet file locally using PyArrow, read it back, and assert that the file schema matches the Glue_Catalog table definition, the row count equals the number of records written, and every non-null field value read back equals the value written.
3. THE schema test suite SHALL use PyArrow only as a development dependency, not a runtime dependency.
4. FOR ALL Normalized_Records produced by the Ingestion_Lambda, writing to Parquet with the Glue schema and reading back SHALL preserve all field values including null values for optional absent fields (round-trip property).
5. THE schema test suite SHALL include at least one test record with all optional fields set to null and at least one test record with all fields populated, to verify both null and non-null round-trip correctness.

### Requirement 12: Infrastructure Validation Tests

**User Story:** As a DevOps engineer, I want automated infrastructure validation, so that CloudFormation templates meet security and correctness standards before deployment.

#### Acceptance Criteria

1. THE infrastructure test suite SHALL pass sam validate --lint without errors for all SAM/CloudFormation template files in the repository.
2. THE infrastructure test suite SHALL pass cfn-lint without errors for all SAM/CloudFormation template files in the repository.
3. THE infrastructure test suite SHALL verify that every S3 bucket resource defines a PublicAccessBlockConfiguration with BlockPublicAcls, BlockPublicPolicy, IgnorePublicAcls, and RestrictPublicBuckets all set to true.
4. THE infrastructure test suite SHALL verify that every S3 bucket resource and every Firehose delivery stream resource has server-side encryption enabled.
5. THE infrastructure test suite SHALL verify that every S3 bucket resource includes a bucket policy statement denying requests where aws:SecureTransport is false.
6. THE infrastructure test suite SHALL verify that no IAM policy statement uses a wildcard Action (e.g., "s3:*", "kms:*", "logs:*", "*") or a wildcard Resource ("*") combined with allow effect.
7. THE infrastructure test suite SHALL verify that the Firehose delivery stream resource has DataFormatConversionConfiguration with Enabled set to true.

### Requirement 13: Integration Testing

**User Story:** As a QA engineer, I want integration tests that validate the deployed dev environment end-to-end, so that the pipeline works correctly before promoting to production.

#### Acceptance Criteria

1. WHEN the dev stack is deployed, THE integration test suite SHALL execute real HTTP requests against the API endpoint obtained from CloudFormation stack outputs and validate responses for HTTP 202, 401, 400, 413, and 404/405 scenarios.
2. WHEN a successful HTTP 202 response is received, THE integration test suite SHALL poll CloudWatch Logs for up to 60 seconds and verify that INGESTION_RECEIVED, INGESTION_VALIDATED, and FIREHOSE_ACCEPTED entries are present for the test request, identified by its unique ingestion_id.
3. WHEN verifying S3 delivery, THE integration test suite SHALL wait up to 330 seconds for the Firehose buffer to flush, then download a Parquet file from the expected S3 prefix, and validate that the file contains all 16 Normalized_Record columns with correct Parquet types matching the Glue_Catalog table definition, and that the test event's field values match the values sent in the original request.
4. WHEN verifying Athena queryability, THE integration test suite SHALL execute a query filtering by the test event's unique ingestion_id, wait up to 60 seconds for query completion, and verify that exactly one row is returned with field values matching the originally submitted test event.
5. THE integration test suite SHALL generate a unique ingestion_id or event_id per test run so that test assertions can isolate their specific event from other data in the environment.

### Requirement 14: Production Smoke Testing

**User Story:** As an operations engineer, I want automated smoke tests after production deployment, so that basic pipeline health is confirmed without polluting analytics data.

#### Acceptance Criteria

1. WHEN a smoke test is triggered, THE Ingestion_Pipeline SHALL accept a request with event_type set to PIPELINE_SMOKE_TEST and return HTTP 202 Accepted.
2. WHEN a smoke test event has been accepted, THE smoke test suite SHALL verify that FIREHOSE_ACCEPTED appears in CloudWatch Logs for that event within 30 seconds of submission.
3. THE Ingestion_Pipeline SHALL preserve the event_type value of PIPELINE_SMOKE_TEST on smoke test records throughout the pipeline so that downstream consumers can filter them out of analytics datasets.
4. IF the FIREHOSE_ACCEPTED log entry does not appear within 30 seconds of smoke test submission, THEN THE smoke test suite SHALL report a failure result indicating the verification timed out.
5. WHEN the smoke test suite completes all verifications, THE smoke test suite SHALL report a final result of pass only if every verification step succeeded, or fail with identification of which step did not succeed.

### Requirement 15: CI/CD Pipeline

**User Story:** As a DevOps engineer, I want an automated CI/CD pipeline defined in CloudFormation, so that code changes are validated, tested, and deployed through consistent stages with manual approval before production.

#### Acceptance Criteria

1. THE CI_CD_Pipeline SHALL be defined in a separate CloudFormation stack (pipeline/codepipeline.yaml).
2. THE CI_CD_Pipeline SHALL use AWS CodeConnections for GitHub source integration without personal access tokens.
3. THE CI_CD_Pipeline SHALL execute stages in order: Source, ValidateAndTest, Package, DeployDev, IntegrationTests, ManualApproval, DeployProd, SmokeTestProd.
4. IF integration tests in the dev environment fail, THEN THE CI_CD_Pipeline SHALL stop pipeline execution and NOT proceed to the ManualApproval or DeployProd stages.
5. THE CI_CD_Pipeline SHALL require manual approval before deploying to production, with an approval action timeout of 7 days after which the pipeline execution fails.
6. THE CI_CD_Pipeline SHALL send a notification to a configurable SNS topic when the pipeline reaches the ManualApproval stage.
7. THE CI_CD_Pipeline SHALL publish JUnit XML test reports and Coverage XML reports to CodeBuild report groups.
8. THE CI_CD_Pipeline SHALL use separate buildspec files: buildspec-ci.yml, buildspec-package.yml, buildspec-integration.yml, buildspec-smoke.yml.
9. THE CI_CD_Pipeline SHALL accept parameters: ConnectionArn, RepositoryOwner, RepositoryName, BranchName (default main), DevStackName, ProdStackName, and ApprovalNotificationArn.
10. THE CI_CD_Pipeline SHALL be deployable via a single CloudFormation create-stack or deploy command.
11. IF the Source stage fails to retrieve code from the configured repository, THEN THE CI_CD_Pipeline SHALL fail the pipeline execution and report the failure to the configured SNS topic.

### Requirement 16: Security Controls

**User Story:** As a security engineer, I want secrets and sensitive data protected throughout the pipeline, so that credentials are never exposed in logs, artifacts, or source code.

#### Acceptance Criteria

1. THE Ingestion_Lambda SHALL retrieve all authentication secrets (API keys, tokens, passwords, and connection strings) from AWS Secrets Manager at runtime rather than from environment variables or source code.
2. THE Ingestion_Pipeline SHALL NOT include secret values in CloudWatch Logs, CodeBuild artifacts, or CloudFormation outputs, including in error messages, stack traces, or debug output.
3. THE CI_CD_Pipeline SHALL NOT use GitHub personal access tokens; it SHALL use AWS CodeConnections exclusively.
4. THE Ingestion_Pipeline SHALL enforce TLS 1.2 or higher for all data in transit between pipeline components and external endpoints.
5. IF the Ingestion_Lambda fails to retrieve a secret from AWS Secrets Manager within 5 seconds, THEN THE Ingestion_Lambda SHALL abort the current operation, log an error message indicating retrieval failure without exposing the secret name or value, and return a failure status to the caller.

### Requirement 17: Configuration Parameters

**User Story:** As a DevOps engineer, I want key operational values exposed as configurable parameters, so that tuning and environment-specific adjustments do not require code changes.

#### Acceptance Criteria

1. THE Ingestion_Pipeline SHALL accept FirehoseBufferIntervalSeconds as a configurable CloudFormation parameter with a default of 300, minimum of 60, and maximum of 900.
2. THE Ingestion_Pipeline SHALL accept FirehoseBufferSizeMB as a configurable CloudFormation parameter with a default of 64, minimum of 64 (required when format conversion is enabled), and maximum of 128.
3. THE Ingestion_Pipeline SHALL accept PartitionProjectionStartDate as a configurable CloudFormation parameter with a default of 2026-01-01 in yyyy-MM-dd format.
4. THE Ingestion_Pipeline SHALL accept ParquetCompression as a configurable CloudFormation parameter with a default of SNAPPY and allowed values of SNAPPY, GZIP, and UNCOMPRESSED.
5. THE Ingestion_Pipeline SHALL accept AthenaBytesScannedCutoffPerQuery as a configurable CloudFormation parameter expressed in bytes.
6. THE Ingestion_Pipeline SHALL accept AthenaResultsRetentionDays as a configurable CloudFormation parameter expressed as a positive integer representing the number of days to retain Athena query results.
7. THE Ingestion_Pipeline SHALL accept EnableAthenaWorkGroup as a configurable CloudFormation parameter of type String with allowed values true and false, defaulting to true.

### Requirement 18: CloudFormation Outputs

**User Story:** As a DevOps engineer, I want complete CloudFormation outputs for all key resources, so that other systems and operators can reference deployed resources programmatically.

#### Acceptance Criteria

1. THE Ingestion_Pipeline CloudFormation stack SHALL output: GlueDatabaseName (the Glue database name), GlueTableName (the Glue table name), GlueTableArn (the full ARN of the Glue table), AthenaWorkGroupName (the Athena WorkGroup name), AthenaResultsLocation (the full S3 URI for Athena query results), ParquetDataPrefix (the full S3 URI for the Parquet data prefix), FirehoseErrorPrefix (the full S3 URI for the Firehose error output prefix).
2. THE CI_CD_Pipeline CloudFormation stack SHALL output: PipelineName (the CodePipeline pipeline name), DevStackName (the name of the deployed dev CloudFormation stack), ProdStackName (the name of the deployed prod CloudFormation stack).
3. IF the EnableAthenaWorkGroup parameter is set to false, THEN THE Ingestion_Pipeline CloudFormation stack SHALL omit AthenaWorkGroupName and AthenaResultsLocation from its outputs using CloudFormation conditions.

### Requirement 19: Documentation

**User Story:** As a developer, I want clear documentation for setup and common operations, so that new team members can deploy and query the system without tribal knowledge.

#### Acceptance Criteria

1. THE repository SHALL include a README that documents the AWS CodeConnections GitHub setup process, covering at minimum: creating the connection in the AWS Console, authorizing the GitHub account, confirming the connection status is Available, and retrieving the Connection ARN for pipeline deployment.
2. THE repository SHALL include a README that documents the initial pipeline deployment procedure, covering at minimum: required tooling prerequisites (AWS CLI, SAM CLI, Python), the command to deploy the CI_CD_Pipeline stack with its required parameters (ConnectionArn, RepositoryOwner, RepositoryName), and verification that the pipeline executes successfully.
3. THE repository SHALL include at least 4 ready-to-copy Athena query examples in the README covering: filtering records by ingestion_date range, filtering records by event_type, extracting a nested field from raw_event_json using json_extract_scalar, and excluding PIPELINE_SMOKE_TEST records from results.
4. THE repository SHALL include a README section that lists all configurable CloudFormation parameters from the Ingestion_Pipeline stack with their names, descriptions, and default values.
