import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit
import os

class CSVToParquetProcessor:
    def __init__(self):
        # Get job parameters
        params = []
        if '--JOB_NAME' in sys.argv:
            params.extend(['JOB_NAME', 'input_path', 'output_path', 'database_name'])
        
        args = getResolvedOptions(sys.argv, params)
        
        # Initialize Glue context
        sc = SparkContext()
        self.context = GlueContext(sc)
        self.spark = self.context.spark_session
        self.job = Job(self.context)
        
        if 'JOB_NAME' in args:
            self.job_name = args['JOB_NAME']
            self.input_path = args['input_path']
            self.output_path = args['output_path']
            self.database_name = args['database_name']
        else:
            # Default values for local testing
            self.job_name = "test"
            self.input_path = "s3://test-bucket/input/sample.csv"
            self.output_path = "s3://test-bucket/output/"
            self.database_name = "test_database"
        
        self.job.init(self.job_name, args)
    
    def run(self):
        """Convert CSV to Parquet with metadata"""
        print(f"Starting CSV to Parquet conversion: {self.job_name}")
        print(f"Input CSV: {self.input_path}")
        print(f"Output path: {self.output_path}")
        
        # Read CSV file using Spark
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(self.input_path)
        
        print(f"Read {df.count()} records from CSV file")
        print("Schema:")
        df.printSchema()
        
        # Add metadata columns
        input_filename = os.path.basename(self.input_path)
        filename_without_ext = os.path.splitext(input_filename)[0]
        
        df_with_metadata = df \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("source_file", lit(input_filename)) \
            .withColumn("processing_job", lit(self.job_name))
        
        # Create output path
        output_location = f"{self.output_path}{filename_without_ext}_processed/"
        
        print(f"Writing to: {output_location}")
        
        # Write to Parquet format
        df_with_metadata.write \
            .mode("overwrite") \
            .parquet(output_location)
        
        print(f"Successfully wrote {df_with_metadata.count()} records to {output_location}")
        
        # Show sample data
        print("Sample processed data:")
        df_with_metadata.show(5, truncate=False)
        
        # Commit the job
        self.job.commit()

if __name__ == '__main__':
    processor = CSVToParquetProcessor()
    processor.run()