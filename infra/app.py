#!/usr/bin/env python3
"""CDK App entry point for Botmaker Log Ingestion Pipeline."""

import os

import aws_cdk as cdk

from stacks.ingestion_stack import IngestionStack
from stacks.pipeline_stack import PipelineStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

# Application stack (dev)
IngestionStack(
    app,
    "BotmakerLogIngestionDev",
    environment="dev",
    env=env,
)

# Application stack (prod) — deployed by the pipeline
IngestionStack(
    app,
    "BotmakerLogIngestionProd",
    environment="prod",
    env=env,
)

# CI/CD Pipeline stack
PipelineStack(
    app,
    "BotmakerLogIngestionPipeline",
    env=env,
)

app.synth()
