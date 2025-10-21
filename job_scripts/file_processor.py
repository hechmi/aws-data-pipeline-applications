import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import current_timestamp, lit
import os

class CSVToIcebergProcessor:
    def __init__(self):
        # Get job parameters
        params = []
        if '--JOB_NAME' in sys.argv:
            params.extend(['JOB_NAME', 'input_path', 'output_path', 'database_name'])
        
        args = getResolvedOptions(sys.argv, params)
        
        # Initialize Spark with Iceberg support
        spark = SparkSession.builder \
            .appName("CSV-to-Iceberg-Processor") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.warehouse", "s3://glue-output-dev-095929019002/iceberg-warehouse/") \
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .getOrCreate()
        
        self.spark = spark
        self.context = GlueContext(spark.sparkContext)
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
        """Convert CSV to Iceberg table"""
        print(f"Starting CSV to Iceberg conversion: {self.job_name}")
        print(f"Input CSV: {self.input_path}")
        print(f"Output database: {self.database_name}")
        
        # Read CSV file
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(self.input_path)
        
        print(f"Read {df.count()} records from CSV file")
        df.printSchema()
        
        # Add metadata columns
        input_filename = os.path.basename(self.input_path)
        filename_without_ext = os.path.splitext(input_filename)[0]
        
        df_with_metadata = df \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("source_file", lit(input_filename)) \
            .withColumn("processing_job", lit(self.job_name))
        
        # Create table name from filename
        table_name = f"{filename_without_ext}_iceberg"
        full_table_name = f"glue_catalog.{self.database_name}.{table_name}"
        
        print(f"Creating Iceberg table: {full_table_name}")
        
        # Write to Iceberg table
        df_with_metadata.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("path", f"s3://glue-output-dev-095929019002/iceberg-warehouse/{self.database_name}/{table_name}") \
            .saveAsTable(full_table_name)
        
        print(f"Successfully created Iceberg table {full_table_name} with {df_with_metadata.count()} records")
        
        # Show sample data
        print("Sample data:")
        df_with_metadata.show(5)
        
        # Commit the job
        self.job.commit()

if __name__ == '__main__':
    processor = CSVToIcebergProcessor()
    processor.run()