#!/usr/bin/env python3
"""
End-to-end testing script for the data pipeline
Tests the complete workflow: S3 upload → Lambda → Glue job → processed output
"""

import boto3
import json
import time
import sys
import uuid
from datetime import datetime

class EndToEndTester:
    def __init__(self, stage, region, account_id, profile):
        self.stage = stage
        self.region = region
        self.account_id = account_id
        self.profile = profile
        
        # AWS clients with profile
        session = boto3.Session(profile_name=profile)
        self.s3 = session.client('s3', region_name=region)
        self.glue = session.client('glue', region_name=region)
        self.logs = session.client('logs', region_name=region)
        
        # Expected resource names
        self.input_bucket = f"glue-input-{stage}-{account_id}"
        self.output_bucket = f"glue-output-{stage}-{account_id}"
        self.job_name = f"FileProcessor-{stage}"
    
    def create_test_files(self):
        """Create test files in different formats"""
        test_files = {}
        
        # JSON test file
        json_data = {
            "id": 1,
            "name": "John Doe",
            "age": 30,
            "city": "New York",
            "timestamp": datetime.now().isoformat()
        }
        test_files["test_data.json"] = json.dumps(json_data, indent=2)
        
        # CSV test file
        csv_data = """name,age,city,department
John Doe,30,New York,Engineering
Jane Smith,25,Los Angeles,Marketing
Bob Johnson,35,Chicago,Sales"""
        test_files["test_data.csv"] = csv_data
        
        return test_files
    
    def upload_test_file(self, filename, content):
        """Upload a test file to the input bucket"""
        print(f"📤 Uploading {filename} to s3://{self.input_bucket}/")
        
        self.s3.put_object(
            Bucket=self.input_bucket,
            Key=filename,
            Body=content,
            ContentType='application/json' if filename.endswith('.json') else 'text/csv'
        )
        
        return f"s3://{self.input_bucket}/{filename}"
    
    def wait_for_job_completion(self, timeout_minutes=10):
        """Wait for Glue job to complete"""
        print(f"⏳ Waiting for Glue job {self.job_name} to complete...")
        
        timeout = time.time() + (timeout_minutes * 60)
        
        while time.time() < timeout:
            try:
                response = self.glue.get_job_runs(JobName=self.job_name, MaxResults=5)
                
                if response['JobRuns']:
                    latest_run = response['JobRuns'][0]
                    status = latest_run['JobRunState']
                    
                    print(f"  Job status: {status}")
                    
                    if status == 'SUCCEEDED':
                        print(f"✅ Job completed successfully!")
                        return True
                    elif status in ['FAILED', 'ERROR', 'TIMEOUT']:
                        print(f"❌ Job failed with status: {status}")
                        return False
                    
            except Exception as e:
                print(f"  Error checking job status: {e}")
            
            time.sleep(30)  # Check every 30 seconds
        
        print(f"❌ Job did not complete within {timeout_minutes} minutes")
        return False
    
    def check_output_files(self, original_filename):
        """Check if processed files were created in output bucket"""
        print(f"🔍 Checking for processed output files...")
        
        # Expected output folder
        filename_without_ext = original_filename.split('.')[0]
        expected_prefix = f"{filename_without_ext}_processed/"
        
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.output_bucket,
                Prefix=expected_prefix
            )
            
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
                print(f"✅ Found {len(files)} output files:")
                for file in files:
                    print(f"  - s3://{self.output_bucket}/{file}")
                return True
            else:
                print(f"❌ No output files found with prefix: {expected_prefix}")
                return False
                
        except Exception as e:
            print(f"❌ Error checking output files: {e}")
            return False
    
    def cleanup_test_files(self, test_files):
        """Clean up test files from input and output buckets"""
        print("🧹 Cleaning up test files...")
        
        # Clean input files
        for filename in test_files.keys():
            try:
                self.s3.delete_object(Bucket=self.input_bucket, Key=filename)
                print(f"  Deleted input: {filename}")
            except Exception as e:
                print(f"  Error deleting {filename}: {e}")
        
        # Clean output files (list and delete all with test prefix)
        for filename in test_files.keys():
            filename_without_ext = filename.split('.')[0]
            prefix = f"{filename_without_ext}_processed/"
            
            try:
                response = self.s3.list_objects_v2(Bucket=self.output_bucket, Prefix=prefix)
                if 'Contents' in response:
                    for obj in response['Contents']:
                        self.s3.delete_object(Bucket=self.output_bucket, Key=obj['Key'])
                        print(f"  Deleted output: {obj['Key']}")
            except Exception as e:
                print(f"  Error cleaning output files: {e}")
    
    def run_end_to_end_test(self):
        """Run complete end-to-end test"""
        print(f"\n🚀 Starting End-to-End Test for {self.stage} environment")
        print(f"Region: {self.region}, Account: {self.account_id}")
        print("=" * 60)
        
        # Create test files
        test_files = self.create_test_files()
        
        success_count = 0
        total_tests = len(test_files)
        
        for filename, content in test_files.items():
            print(f"\n📋 Testing file: {filename}")
            print("-" * 40)
            
            try:
                # Upload file
                self.upload_test_file(filename, content)
                
                # Wait for job completion
                if self.wait_for_job_completion():
                    # Check output
                    if self.check_output_files(filename):
                        print(f"✅ Test PASSED for {filename}")
                        success_count += 1
                    else:
                        print(f"❌ Test FAILED for {filename} - no output files")
                else:
                    print(f"❌ Test FAILED for {filename} - job did not complete")
                    
            except Exception as e:
                print(f"❌ Test FAILED for {filename} - error: {e}")
        
        # Cleanup
        self.cleanup_test_files(test_files)
        
        # Summary
        print("\n" + "=" * 60)
        print("📋 END-TO-END TEST SUMMARY:")
        print(f"  ✅ Passed: {success_count}/{total_tests}")
        print(f"  ❌ Failed: {total_tests - success_count}/{total_tests}")
        
        if success_count == total_tests:
            print(f"\n🎉 All end-to-end tests PASSED for {self.stage}!")
            return 0
        else:
            print(f"\n💥 Some end-to-end tests FAILED for {self.stage}!")
            return 1

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python test_end_to_end.py <stage> <region> <account_id> <profile>")
        print("Example: python test_end_to_end.py dev us-west-2 123456789012 dev-west")
        sys.exit(1)
    
    stage = sys.argv[1]
    region = sys.argv[2]
    account_id = sys.argv[3]
    profile = sys.argv[4]
    
    tester = EndToEndTester(stage, region, account_id, profile)
    exit_code = tester.run_end_to_end_test()
    sys.exit(exit_code)