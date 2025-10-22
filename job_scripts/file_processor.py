import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit
import os

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path', 'database_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting CSV to Parquet conversion: {args['JOB_NAME']}")
print(f"Input CSV: {args['input_path']}")
print(f"Output path: {args['output_path']}")
print("End-to-end pipeline test - Version 1.0")

try:
    # Read CSV file using Spark
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(args['input_path'])
    
    print(f"Read {df.count()} records from CSV file")
    print("Schema:")
    df.printSchema()
    
    # Add metadata columns
    input_filename = os.path.basename(args['input_path'])
    filename_without_ext = os.path.splitext(input_filename)[0]
    
    df_with_metadata = df \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("source_file", lit(input_filename)) \
        .withColumn("processing_job", lit(args['JOB_NAME']))
    
    # Create output path
    output_location = f"{args['output_path']}{filename_without_ext}_processed/"
    
    print(f"Writing to: {output_location}")
    
    # Write to Parquet format
    df_with_metadata.write \
        .mode("overwrite") \
        .parquet(output_location)
    
    print(f"Successfully wrote {df_with_metadata.count()} records to {output_location}")
    
    # Show sample data
    print("Sample processed data:")
    df_with_metadata.show(5, truncate=False)
    
    print("Job completed successfully!")
    
except Exception as e:
    print(f"Error processing file: {str(e)}")
    raise e

finally:
    job.commit()