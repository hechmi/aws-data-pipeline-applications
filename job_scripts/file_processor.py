import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.sql.functions import current_timestamp, input_file_name, lit
import os

class FileProcessor:
    def __init__(self):
        # Get job parameters
        params = []
        if '--JOB_NAME' in sys.argv:
            params.extend(['JOB_NAME', 'input_path', 'output_path', 'database_name'])
        
        args = getResolvedOptions(sys.argv, params)
        
        # Initialize Glue context
        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)
        
        if 'JOB_NAME' in args:
            self.job_name = args['JOB_NAME']
            self.input_path = args['input_path']
            self.output_path = args['output_path']
            self.database_name = args['database_name']
        else:
            # Default values for local testing
            self.job_name = "test"
            self.input_path = "s3://test-bucket/input/sample.json"
            self.output_path = "s3://test-bucket/output/"
            self.database_name = "test_database"
        
        self.job.init(self.job_name, args)
    
    def detect_file_format(self, file_path):
        """Detect file format based on extension"""
        if file_path.lower().endswith('.json'):
            return 'json'
        elif file_path.lower().endswith('.csv'):
            return 'csv'
        elif file_path.lower().endswith('.parquet'):
            return 'parquet'
        else:
            return 'json'  # Default to JSON
    
    def run(self):
        """Main job execution logic"""
        print(f"Starting job: {self.job_name}")
        print(f"Input path: {self.input_path}")
        print(f"Output path: {self.output_path}")
        print(f"Database: {self.database_name}")
        
        # Detect file format
        file_format = self.detect_file_format(self.input_path)
        print(f"Detected file format: {file_format}")
        
        # Read input file based on format
        if file_format == 'csv':
            input_dyf = self.context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [self.input_path]
                },
                format="csv",
                format_options={
                    "withHeader": True,
                    "separator": ","
                }
            )
        elif file_format == 'parquet':
            input_dyf = self.context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [self.input_path]
                },
                format="parquet"
            )
        else:  # JSON or default
            input_dyf = self.context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [self.input_path]
                },
                format="json"
            )
        
        print(f"Read {input_dyf.count()} records from input file")
        
        # Convert to DataFrame for processing
        df = input_dyf.toDF()
        
        # Add metadata columns
        input_filename = os.path.basename(self.input_path)
        df_with_metadata = df.withColumn("processed_at", current_timestamp()) \
                            .withColumn("source_file", lit(input_filename)) \
                            .withColumn("processing_job", lit(self.job_name))
        
        # Convert back to DynamicFrame
        processed_dyf = self.context.create_dynamic_frame.from_dataframe(
            df_with_metadata, self.context, "processed_data"
        )
        
        # Generate output file name based on input file
        filename_without_ext = os.path.splitext(input_filename)[0]
        output_location = f"{self.output_path}{filename_without_ext}_processed/"
        
        # Write to S3 in Parquet format (optimized for analytics)
        self.context.write_dynamic_frame.from_options(
            frame=processed_dyf,
            connection_type="s3",
            connection_options={
                "path": output_location
            },
            format="parquet"
        )
        
        print(f"Successfully wrote {processed_dyf.count()} records to {output_location}")
        
        # Commit the job
        self.job.commit()

if __name__ == '__main__':
    processor = FileProcessor()
    processor.run()