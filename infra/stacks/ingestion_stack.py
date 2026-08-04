"""CDK Stack for Botmaker Log Ingestion Pipeline.

Defines: S3 bucket, Secrets Manager, API Gateway HTTP API, Lambda function,
Firehose delivery stream (JSON→Parquet), Glue Data Catalog (with partition
projection), Athena WorkGroup, and all supporting IAM roles.
"""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as apigwv2_integrations,
    aws_athena as athena,
    aws_glue as glue,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct


class IngestionStack(Stack):
    """Main application stack for the Botmaker log ingestion pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        environment: str = "dev",
        firehose_buffer_interval_seconds: int = 300,
        firehose_buffer_size_mb: int = 64,
        partition_projection_start_date: str = "2026-01-01",
        parquet_compression: str = "SNAPPY",
        enable_athena_workgroup: bool = True,
        log_payload: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # =====================================================================
        # S3 Bucket — Parquet data, Firehose errors, Athena results
        # =====================================================================
        logs_bucket = s3.Bucket(
            self,
            "LogsBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
        )

        # =====================================================================
        # Secrets Manager — Webhook Bearer token
        # =====================================================================
        webhook_secret = secretsmanager.Secret(
            self,
            "WebhookSecret",
            secret_name=f"botmaker-log-ingestion/{environment}/webhook-secret",
            description="Bearer token for Botmaker webhook authentication",
        )

        # =====================================================================
        # CloudWatch Log Groups (30-day retention)
        # =====================================================================
        lambda_log_group = logs.LogGroup(
            self,
            "LambdaLogGroup",
            log_group_name=f"/aws/lambda/botmaker-log-ingestion-{environment}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        api_access_log_group = logs.LogGroup(
            self,
            "ApiAccessLogGroup",
            log_group_name=f"/aws/apigateway/botmaker-log-ingestion-{environment}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        firehose_log_group = logs.LogGroup(
            self,
            "FirehoseLogGroup",
            log_group_name=f"/aws/firehose/botmaker-log-ingestion-{environment}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        firehose_log_stream = logs.LogStream(
            self,
            "FirehoseLogStream",
            log_group=firehose_log_group,
            log_stream_name="S3Delivery",
        )

        # =====================================================================
        # Glue Data Catalog — Database and Table
        # =====================================================================
        database_name = f"botmaker_log_ingestion_{environment}"

        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                description="Botmaker log ingestion pipeline — normalized events",
            ),
        )

        glue_table = glue.CfnTable(
            self,
            "GlueTable",
            catalog_id=self.account,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="botmaker_logs",
                description="Normalized Botmaker webhook events stored as Parquet",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "projection.enabled": "true",
                    "projection.ingestion_date.type": "date",
                    "projection.ingestion_date.format": "yyyy-MM-dd",
                    "projection.ingestion_date.range": f"{partition_projection_start_date},NOW",
                    "projection.ingestion_date.interval": "1",
                    "projection.ingestion_date.interval.unit": "DAYS",
                    "storage.location.template": f"s3://{logs_bucket.bucket_name}/data/ingestion_date=${{ingestion_date}}/",
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="ingestion_date", type="string"),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{logs_bucket.bucket_name}/data/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="ingestion_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="event_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="request_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="received_at", type="string"),
                        glue.CfnTable.ColumnProperty(name="schema_version", type="int"),
                        glue.CfnTable.ColumnProperty(name="environment", type="string"),
                        glue.CfnTable.ColumnProperty(name="event_type", type="string"),
                        glue.CfnTable.ColumnProperty(name="conversation_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="message_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="user_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="order_form_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="source_channel", type="string"),
                        glue.CfnTable.ColumnProperty(name="source_bot", type="string"),
                        glue.CfnTable.ColumnProperty(name="severity", type="string"),
                        glue.CfnTable.ColumnProperty(name="error_code", type="string"),
                        glue.CfnTable.ColumnProperty(name="raw_event_json", type="string"),
                    ],
                ),
            ),
        )
        glue_table.add_dependency(glue_database)

        # =====================================================================
        # Firehose IAM Role
        # =====================================================================
        firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        logs_bucket.grant_put(firehose_role)

        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetTable", "glue:GetTableVersions"],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{database_name}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{database_name}/*",
                ],
            )
        )

        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:PutLogEvents"],
                resources=[firehose_log_group.log_group_arn + ":*"],
            )
        )

        # =====================================================================
        # Amazon Data Firehose — Direct PUT, JSON→Parquet
        # =====================================================================
        delivery_stream_name = f"botmaker-log-ingestion-{environment}"

        delivery_stream = firehose.CfnDeliveryStream(
            self,
            "DeliveryStream",
            delivery_stream_name=delivery_stream_name,
            delivery_stream_type="DirectPut",
            delivery_stream_encryption_configuration_input=firehose.CfnDeliveryStream.DeliveryStreamEncryptionConfigurationInputProperty(
                key_type="AWS_OWNED_CMK",
            ),
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=logs_bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                prefix="data/ingestion_date=!{timestamp:yyyy-MM-dd}/",
                error_output_prefix="firehose-errors/type=!{firehose:error-output-type}/ingestion_date=!{timestamp:yyyy-MM-dd}/",
                custom_time_zone="America/Bogota",
                file_extension=".parquet",
                compression_format="UNCOMPRESSED",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    size_in_m_bs=firehose_buffer_size_mb,
                    interval_in_seconds=firehose_buffer_interval_seconds,
                ),
                cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=True,
                    log_group_name=firehose_log_group.log_group_name,
                    log_stream_name="S3Delivery",
                ),
                data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                    enabled=True,
                    input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                        deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                            open_x_json_ser_de=firehose.CfnDeliveryStream.OpenXJsonSerDeProperty(),
                        ),
                    ),
                    output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                        serializer=firehose.CfnDeliveryStream.SerializerProperty(
                            parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty(
                                compression=parquet_compression,
                            ),
                        ),
                    ),
                    schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                        database_name=database_name,
                        table_name="botmaker_logs",
                        role_arn=firehose_role.role_arn,
                        region=self.region,
                        version_id="LATEST",
                    ),
                ),
            ),
        )
        delivery_stream.add_dependency(glue_table)
        delivery_stream.node.add_dependency(firehose_log_stream)
        # Ensure IAM policy is fully propagated before Firehose tries to use it
        delivery_stream.node.add_dependency(firehose_role)

        # =====================================================================
        # Lambda Function
        # =====================================================================
        ingestion_function = lambda_.Function(
            self,
            "IngestionFunction",
            function_name=f"botmaker-log-ingestion-{environment}",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("src"),
            memory_size=256,
            timeout=Duration.seconds(10),
            reserved_concurrent_executions=10,
            log_group=lambda_log_group,
            environment={
                "ENVIRONMENT": environment,
                "FIREHOSE_STREAM_NAME": delivery_stream_name,
                "SECRET_ID": webhook_secret.secret_arn,
                "LOG_LEVEL": "INFO",
                "LOG_PAYLOAD": str(log_payload).lower(),
            },
        )

        # Lambda IAM — least privilege
        ingestion_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["firehose:PutRecord"],
                resources=[
                    f"arn:aws:firehose:{self.region}:{self.account}:deliverystream/{delivery_stream_name}"
                ],
            )
        )

        webhook_secret.grant_read(ingestion_function)

        # =====================================================================
        # API Gateway HTTP API
        # =====================================================================
        http_api = apigwv2.HttpApi(
            self,
            "WebhookHttpApi",
            api_name=f"botmaker-log-ingestion-{environment}",
            description="HTTP API for Botmaker webhook ingestion",
        )

        # Lambda integration
        integration = apigwv2_integrations.HttpLambdaIntegration(
            "LambdaIntegration",
            handler=ingestion_function,
        )

        http_api.add_routes(
            path="/webhook",
            methods=[apigwv2.HttpMethod.POST],
            integration=integration,
        )

        # Access logging for the default stage
        cfn_stage = http_api.default_stage.node.default_child
        cfn_stage.access_log_settings = apigwv2.CfnStage.AccessLogSettingsProperty(
            destination_arn=api_access_log_group.log_group_arn,
            format='{"requestId":"$context.requestId","ip":"$context.identity.sourceIp","requestTime":"$context.requestTime","httpMethod":"$context.httpMethod","routeKey":"$context.routeKey","status":"$context.status","protocol":"$context.protocol","responseLength":"$context.responseLength"}',
        )

        # Grant API Gateway permission to write to the log group
        api_access_log_group.grant_write(iam.ServicePrincipal("apigateway.amazonaws.com"))

        # =====================================================================
        # Athena WorkGroup (conditional)
        # =====================================================================
        if enable_athena_workgroup:
            athena_workgroup = athena.CfnWorkGroup(
                self,
                "AthenaWorkGroup",
                name=f"botmaker-log-ingestion-{environment}",
                description="Athena WorkGroup for querying Botmaker logs",
                state="ENABLED",
                work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                    enforce_work_group_configuration=True,
                    publish_cloud_watch_metrics_enabled=True,
                    result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                        output_location=f"s3://{logs_bucket.bucket_name}/athena-results/",
                        encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                            encryption_option="SSE_S3",
                        ),
                    ),
                ),
            )

            CfnOutput(self, "AthenaWorkGroupName", value=athena_workgroup.name)
            CfnOutput(
                self,
                "AthenaResultsLocation",
                value=f"s3://{logs_bucket.bucket_name}/athena-results/",
            )

        # =====================================================================
        # Outputs
        # =====================================================================
        CfnOutput(self, "ApiEndpoint", value=http_api.api_endpoint)
        CfnOutput(self, "LogsBucketName", value=logs_bucket.bucket_name)
        CfnOutput(self, "GlueDatabaseName", value=database_name)
        CfnOutput(self, "GlueTableName", value="botmaker_logs")
        CfnOutput(
            self,
            "ParquetDataPrefix",
            value=f"s3://{logs_bucket.bucket_name}/data/",
        )
        CfnOutput(
            self,
            "FirehoseErrorPrefix",
            value=f"s3://{logs_bucket.bucket_name}/firehose-errors/",
        )
        CfnOutput(self, "FirehoseStreamName", value=delivery_stream_name)
        CfnOutput(self, "WebhookSecretArn", value=webhook_secret.secret_arn)
