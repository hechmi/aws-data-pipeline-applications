from typing import Dict
# Test change to verify automatic application pipeline triggering
from aws_cdk import (
    Stack,
    Duration,
    Fn,
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
        
        # Import infrastructure resources using CloudFormation exports
        self.input_bucket_name = Fn.import_value(f"InputBucket-{stage}")
        self.output_bucket_name = Fn.import_value(f"OutputBucket-{stage}")
        self.assets_bucket_name = Fn.import_value(f"AssetsBucket-{stage}")
        self.glue_database_name = Fn.import_value(f"GlueDatabase-{stage}")
        
        # Import Glue job role from infrastructure stack
        glue_job_role_arn = Fn.import_value(f"GlueJobRole-{stage}")
        self.glue_job_role = iam.Role.from_role_arn(
            self, f"ImportedGlueJobRole-{stage}",
            role_arn=glue_job_role_arn
        )
        
        # Create generic file processor job (event-driven)
        self.file_processor_job = glue.Job(self, f"FileProcessor-{stage}",
            job_name=f"FileProcessor-{stage}",  # Explicit name for Lambda trigger
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V4_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(
                    path.join(path.dirname(__file__), "../job_scripts/file_processor.py")
                )
            ),
            role=self.glue_job_role,
            description=f"Generic file processor for {stage} environment - triggered by S3 uploads",
            default_arguments={
                "--job-language": "python"
            },
            max_concurrent_runs=5,  # Allow multiple files to be processed simultaneously
            timeout=Duration.hours(2)
        )
        
        # Keep the original legislators job for backward compatibility
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
