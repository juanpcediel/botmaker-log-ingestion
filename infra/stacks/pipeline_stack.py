"""CDK Stack for CI/CD Pipeline — CodePipeline with 8 stages."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    CfnOutput,
    Stack,
    aws_codebuild as codebuild,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class PipelineStack(Stack):
    """CI/CD Pipeline: Source → ValidateAndTest → Package → DeployDev →
    IntegrationTests → ManualApproval → DeployProd → SmokeTestProd.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        connection_arn: str | None = None,
        repository_owner: str = "your-org",
        repository_name: str = "botmaker-log-ingestion",
        branch_name: str = "main",
        dev_stack_name: str = "BotmakerLogIngestionDev",
        prod_stack_name: str = "BotmakerLogIngestionProd",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Read from context if not passed directly
        connection_arn = connection_arn or self.node.try_get_context("connectionArn") or "PLACEHOLDER"
        repository_owner = self.node.try_get_context("repositoryOwner") or repository_owner
        repository_name = self.node.try_get_context("repositoryName") or repository_name
        branch_name = self.node.try_get_context("branchName") or branch_name

        # =====================================================================
        # Artifact Bucket
        # =====================================================================
        artifact_bucket = s3.Bucket(
            self,
            "ArtifactBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=cdk.Duration.days(30)),
            ],
        )

        # =====================================================================
        # CodeBuild Projects
        # =====================================================================
        validate_and_test_project = codebuild.PipelineProject(
            self,
            "ValidateAndTest",
            project_name="botmaker-ingestion-validate-test",
            build_spec=codebuild.BuildSpec.from_source_filename("buildspecs/buildspec-ci.yml"),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.SMALL,
            ),
            timeout=cdk.Duration.minutes(20),
        )

        package_project = codebuild.PipelineProject(
            self,
            "Package",
            project_name="botmaker-ingestion-package",
            build_spec=codebuild.BuildSpec.from_source_filename("buildspecs/buildspec-package.yml"),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.SMALL,
            ),
            timeout=cdk.Duration.minutes(15),
        )

        integration_test_project = codebuild.PipelineProject(
            self,
            "IntegrationTests",
            project_name="botmaker-ingestion-integration",
            build_spec=codebuild.BuildSpec.from_source_filename("buildspecs/buildspec-integration.yml"),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.SMALL,
                environment_variables={
                    "STACK_NAME": codebuild.BuildEnvironmentVariable(value=dev_stack_name),
                },
            ),
            timeout=cdk.Duration.minutes(30),
        )

        smoke_test_project = codebuild.PipelineProject(
            self,
            "SmokeTests",
            project_name="botmaker-ingestion-smoke",
            build_spec=codebuild.BuildSpec.from_source_filename("buildspecs/buildspec-smoke.yml"),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.SMALL,
                environment_variables={
                    "STACK_NAME": codebuild.BuildEnvironmentVariable(value=prod_stack_name),
                },
            ),
            timeout=cdk.Duration.minutes(15),
        )

        # Grant CodeBuild projects permissions for integration/smoke tests
        for project in [integration_test_project, smoke_test_project]:
            project.add_to_role_policy(
                iam.PolicyStatement(
                    actions=[
                        "cloudformation:DescribeStacks",
                        "cloudformation:ListStackResources",
                    ],
                    resources=["*"],
                )
            )
            project.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:botmaker-log-ingestion/*"],
                )
            )
            project.add_to_role_policy(
                iam.PolicyStatement(
                    actions=[
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                    ],
                    resources=[f"arn:aws:athena:{self.region}:{self.account}:workgroup/*"],
                )
            )
            project.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
                    resources=["arn:aws:s3:::botmaker-log-ingestion-*", "arn:aws:s3:::botmaker-log-ingestion-*/*"],
                )
            )
            project.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["glue:GetTable", "glue:GetDatabase"],
                    resources=[
                        f"arn:aws:glue:{self.region}:{self.account}:catalog",
                        f"arn:aws:glue:{self.region}:{self.account}:database/*",
                        f"arn:aws:glue:{self.region}:{self.account}:table/*/*",
                    ],
                )
            )
            project.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["logs:FilterLogEvents", "logs:DescribeLogGroups"],
                    resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:*"],
                )
            )

        # =====================================================================
        # Pipeline Artifacts
        # =====================================================================
        source_output = codepipeline.Artifact("SourceOutput")
        test_output = codepipeline.Artifact("TestOutput")
        package_output = codepipeline.Artifact("PackageOutput")

        # =====================================================================
        # Pipeline
        # =====================================================================
        pipeline = codepipeline.Pipeline(
            self,
            "Pipeline",
            pipeline_name="botmaker-log-ingestion",
            artifact_bucket=artifact_bucket,
            restart_execution_on_update=False,
            stages=[
                # Stage 1: Source
                codepipeline.StageProps(
                    stage_name="Source",
                    actions=[
                        codepipeline_actions.CodeStarConnectionsSourceAction(
                            action_name="GitHub",
                            connection_arn=connection_arn,
                            owner=repository_owner,
                            repo=repository_name,
                            branch=branch_name,
                            output=source_output,
                            trigger_on_push=True,
                        ),
                    ],
                ),
                # Stage 2: ValidateAndTest
                codepipeline.StageProps(
                    stage_name="ValidateAndTest",
                    actions=[
                        codepipeline_actions.CodeBuildAction(
                            action_name="ValidateAndTest",
                            project=validate_and_test_project,
                            input=source_output,
                            outputs=[test_output],
                        ),
                    ],
                ),
                # Stage 3: Package
                codepipeline.StageProps(
                    stage_name="Package",
                    actions=[
                        codepipeline_actions.CodeBuildAction(
                            action_name="Package",
                            project=package_project,
                            input=source_output,
                            outputs=[package_output],
                        ),
                    ],
                ),
                # Stage 4: DeployDev
                codepipeline.StageProps(
                    stage_name="DeployDev",
                    actions=[
                        codepipeline_actions.CloudFormationCreateUpdateStackAction(
                            action_name="DeployDev",
                            stack_name=dev_stack_name,
                            template_path=package_output.at_path("packaged-template.yaml"),
                            admin_permissions=False,
                            cfn_capabilities=[
                                cdk.CfnCapabilities.NAMED_IAM,
                                cdk.CfnCapabilities.AUTO_EXPAND,
                            ],
                            parameter_overrides={"Environment": "dev"},
                        ),
                    ],
                ),
                # Stage 5: IntegrationTests
                codepipeline.StageProps(
                    stage_name="IntegrationTests",
                    actions=[
                        codepipeline_actions.CodeBuildAction(
                            action_name="RunIntegrationTests",
                            project=integration_test_project,
                            input=source_output,
                        ),
                    ],
                ),
                # Stage 6: ManualApproval
                codepipeline.StageProps(
                    stage_name="ManualApproval",
                    actions=[
                        codepipeline_actions.ManualApprovalAction(
                            action_name="Approve",
                            additional_information="Integration tests passed. Approve production deployment.",
                        ),
                    ],
                ),
                # Stage 7: DeployProd
                codepipeline.StageProps(
                    stage_name="DeployProd",
                    actions=[
                        codepipeline_actions.CloudFormationCreateUpdateStackAction(
                            action_name="DeployProd",
                            stack_name=prod_stack_name,
                            template_path=package_output.at_path("packaged-template.yaml"),
                            admin_permissions=False,
                            cfn_capabilities=[
                                cdk.CfnCapabilities.NAMED_IAM,
                                cdk.CfnCapabilities.AUTO_EXPAND,
                            ],
                            parameter_overrides={"Environment": "prod"},
                        ),
                    ],
                ),
                # Stage 8: SmokeTestProd
                codepipeline.StageProps(
                    stage_name="SmokeTestProd",
                    actions=[
                        codepipeline_actions.CodeBuildAction(
                            action_name="RunSmokeTests",
                            project=smoke_test_project,
                            input=source_output,
                        ),
                    ],
                ),
            ],
        )

        # =====================================================================
        # Outputs
        # =====================================================================
        CfnOutput(self, "PipelineName", value=pipeline.pipeline_name)
        CfnOutput(self, "DevStackName", value=dev_stack_name)
        CfnOutput(self, "ProdStackName", value=prod_stack_name)
