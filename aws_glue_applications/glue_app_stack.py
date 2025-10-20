from typing import Dict
from aws_cdk import (
    Stack,
    Duration,
    aws_glue_alpha as glue,
    aws_iam as iam
)
from constructs import Construct
from os import path

class GlueAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, config: Dict, stage: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.stage = stage
        self.config = config
        
        # Import infrastructure resources
        self.data_bucket_name = config['infrastructure']['exports']['dataBucket'].replace('dev', stage)
        self.assets_bucket_name = config['infrastructure']['exports']['assetsBucket'].replace('dev', stage)
        self.glue_database_name = config['infrastructure']['exports']['glueDatabase'].replace('dev', stage)
        
        # Import Glue job role from infrastructure stack using ARN
        # Use environment-specific role ARN
        if stage == "dev":
            glue_job_role_arn = f"arn:aws:iam::{config[f'{stage}Account']['awsAccountId']}:role/DevInfraStage-Infrastructure-GlueJobRoledev3BDDF23C-w4nsvNMzUqUo"
        else:  # prod
            glue_job_role_arn = f"arn:aws:iam::{config[f'{stage}Account']['awsAccountId']}:role/ProdInfraStage-Infrastructu-GlueJobRoleprod8C99FE33-3ApfcykBh1Wf"
        
        self.glue_job_role = iam.Role.from_role_arn(
            self, f"ImportedGlueJobRole-{stage}",
            role_arn=glue_job_role_arn
        )
        
        # Create Glue job for processing legislators data
        self.process_legislators_job = glue.Job(self, f"ProcessLegislators-{stage}",
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V4_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(
                    path.join(path.dirname(__file__), "../job_scripts/process_legislators.py")
                )
            ),
            role=self.glue_job_role,
            description=f"Process legislators data for {stage} environment",
            default_arguments={
                "--input_location": config['applications'][stage]['jobs']['ProcessLegislators']['inputLocation'],
                "--output_location": config['applications'][stage]['jobs']['ProcessLegislators']['outputLocation'],
                "--database_name": self.glue_database_name,
                "--enable-metrics": "",
                "--enable-continuous-cloudwatch-log": "",
                "--job-language": "python"
            },
            max_concurrent_runs=1,
            timeout=Duration.hours(1)
        )
    
    @property
    def process_legislators_job_name(self):
        return self.process_legislators_job.job_name
