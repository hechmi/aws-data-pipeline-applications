import pytest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import file_processor
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

@pytest.fixture(scope="module", autouse=True)
def glue_context():
    sys.argv.append('--JOB_NAME')
    sys.argv.append('test_file_processor')

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args['JOB_NAME'], args)

    yield(context)

def test_detect_file_format():
    processor = file_processor.FileProcessor()
    
    assert processor.detect_file_format("test.json") == "json"
    assert processor.detect_file_format("test.csv") == "csv"
    assert processor.detect_file_format("test.parquet") == "parquet"
    assert processor.detect_file_format("test.txt") == "json"  # Default
    assert processor.detect_file_format("TEST.JSON") == "json"  # Case insensitive

def test_processor_initialization():
    processor = file_processor.FileProcessor()
    
    assert processor.job_name == "test"
    assert processor.input_path == "s3://test-bucket/input/sample.json"
    assert processor.output_path == "s3://test-bucket/output/"
    assert processor.database_name == "test_database"

def test_processor_with_parameters():
    # Simulate job parameters
    original_argv = sys.argv.copy()
    sys.argv = [
        'file_processor.py',
        '--JOB_NAME', 'test_job',
        '--input_path', 's3://my-bucket/input/data.csv',
        '--output_path', 's3://my-bucket/output/',
        '--database_name', 'my_database'
    ]
    
    try:
        processor = file_processor.FileProcessor()
        
        assert processor.job_name == "test_job"
        assert processor.input_path == "s3://my-bucket/input/data.csv"
        assert processor.output_path == "s3://my-bucket/output/"
        assert processor.database_name == "my_database"
        
        # Test file format detection with the parameter
        assert processor.detect_file_format(processor.input_path) == "csv"
        
    finally:
        sys.argv = original_argv