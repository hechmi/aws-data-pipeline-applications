import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *

class ProcessLegislators:
    def __init__(self):
        # Get job parameters
        params = []
        if '--JOB_NAME' in sys.argv:
            params.extend(['JOB_NAME', 'input_location', 'output_location', 'database_name'])
        
        args = getResolvedOptions(sys.argv, params)
        
        # Initialize Glue context
        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)
        
        if 'JOB_NAME' in args:
            self.job_name = args['JOB_NAME']
            self.input_location = args['input_location']
            self.output_location = args['output_location']
            self.database_name = args['database_name']
        else:
            # Default values for local testing
            self.job_name = "test"
            self.input_location = "s3://awsglue-datasets/examples/us-legislators/all"
            self.output_location = "s3://test-bucket/output/"
            self.database_name = "test_database"
        
        self.job.init(self.job_name, args)
    
    def run(self):
        """Main job execution logic"""
        print(f"Starting job: {self.job_name}")
        print(f"Input location: {self.input_location}")
        print(f"Output location: {self.output_location}")
        print(f"Database: {self.database_name}")
        
        # Read legislators data
        legislators_dyf = self.context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [f"{self.input_location}/persons.json"],
                "recurse": True
            },
            format="json"
        )
        
        print(f"Read {legislators_dyf.count()} records from input")
        
        # Transform data - select relevant fields
        transformed_dyf = legislators_dyf.select_fields([
            "id", "name", "bio", "terms"
        ])
        
        # Convert to DataFrame for additional processing
        df = transformed_dyf.toDF()
        
        # Add processing timestamp
        from pyspark.sql.functions import current_timestamp
        df_with_timestamp = df.withColumn("processed_at", current_timestamp())
        
        # Convert back to DynamicFrame
        final_dyf = self.context.create_dynamic_frame.from_dataframe(
            df_with_timestamp, self.context, "final_legislators"
        )
        
        # Write to S3 in Parquet format
        self.context.write_dynamic_frame.from_options(
            frame=final_dyf,
            connection_type="s3",
            connection_options={
                "path": self.output_location,
                "partitionKeys": []
            },
            format="parquet"
        )
        
        print(f"Successfully wrote {final_dyf.count()} records to {self.output_location}")
        
        # Commit the job
        self.job.commit()

if __name__ == '__main__':
    processor = ProcessLegislators()
    processor.run()
