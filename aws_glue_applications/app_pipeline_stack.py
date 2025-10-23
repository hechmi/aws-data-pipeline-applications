from typing import Dict
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_codebuild as codebuild
)
from constructs import Construct
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, CodeBuildStep, CodeBuildOptions, ManualApprovalStep
from aws_glue_applications.glue_app_stage import GlueAppStage

# TODO: Replace these placeholders with your actual values
GITHUB_REPO = "YOUR_GITHUB_USERNAME/YOUR_APPLICATION_REPO_NAME"
GITHUB_BRANCH = "main"
# Dedicated connection for application repository
GITHUB_CONNECTION_ARN = "arn:aws:codeconnections:YOUR_REGION:YOUR_PIPELINE_ACCOUNT:connection/YOUR_CONNECTION_ID"

class AppPipelineStack(Stack):
    # Testing automatic pipeline triggering
    # Second test for auto-trigger functionality
    # Third attempt - testing webhook trigger
    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        source = CodePipelineSource.connection(
            GITHUB_REPO,
            GITHUB_BRANCH,
            connection_arn=GITHUB_CONNECTION_ARN,
            trigger_on_push=True
        )

        pipeline = CodePipeline(self, "GlueAppPipeline",
            pipeline_name="GlueAppPipeline",
            cross_account_keys=True,
            docker_enabled_for_synth=True,
            synth=CodeBuildStep("CdkSynth",
                input=source,
                install_commands=[
                    "pip install -r requirements.txt",
                    "pip install -r requirements-dev.txt",
                    "npm install -g aws-cdk",
                ],
                commands=[
                    "cdk synth",
                ],
                build_environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_7_0
                )
            ),
            code_build_defaults=CodeBuildOptions(
                build_environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_7_0
                )
            )
        )

        # Add development application stage
        dev_app_stage = GlueAppStage(self, "DevAppStage", config=config, stage="dev", 
            env=cdk.Environment(
                account=str(config['devAccount']['awsAccountId']),
                region=config['devAccount']['awsRegion']
            ))
        pipeline.add_stage(dev_app_stage)

        # Add production application stage with manual approval
        prod_app_stage = GlueAppStage(self, "ProdAppStage", config=config, stage="prod", 
            env=cdk.Environment(
                account=str(config['prodAccount']['awsAccountId']),
                region=config['prodAccount']['awsRegion']
            ))
        pipeline.add_stage(prod_app_stage, 
            pre=[ManualApprovalStep("ApproveProductionApp")])
