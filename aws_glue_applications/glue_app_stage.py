from typing import Dict
import aws_cdk as cdk
from constructs import Construct
from aws_glue_applications.glue_app_stack import GlueAppStack

class GlueAppStage(cdk.Stage):
    def __init__(self, scope: Construct, construct_id: str, config: Dict, stage: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Create the Glue application stack
        self.glue_app_stack = GlueAppStack(
            self, "GlueAppStack",
            config=config,
            stage=stage
        )
    
    @property
    def process_legislators_job_name(self):
        return self.glue_app_stack.process_legislators_job_name
