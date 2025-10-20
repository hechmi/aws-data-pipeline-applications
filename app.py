#!/usr/bin/env python3
import os
import aws_cdk as cdk
from aws_glue_applications.app_pipeline_stack import AppPipelineStack
from aws_glue_applications.config_loader import load_config

app = cdk.App()

# Load configuration
config = load_config()

# Create application pipeline stack
AppPipelineStack(
    app, 
    "AppPipelineStack",
    config=config,
    env=cdk.Environment(
        account=str(config['pipelineAccount']['awsAccountId']),
        region=config['pipelineAccount']['awsRegion']
    )
)

app.synth()
