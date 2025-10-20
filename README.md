# AWS Data Pipeline Applications

This repository contains the application layer for AWS Glue data processing pipelines.

<!-- Test automatic triggering -->

## Architecture

This repository is part of a dual-pipeline architecture:
- **Infrastructure Repository**: Deploys S3 buckets, IAM roles, Glue databases, EventBridge rules
- **Application Repository** (this repo): Deploys Glue jobs and data processing logic

## Structure

```
aws-data-pipeline-applications/
├── app.py                           # CDK app entry point
├── aws_glue_applications/           # Python package
│   ├── app_pipeline_stack.py        # CI/CD pipeline for applications
│   ├── glue_app_stack.py           # Glue jobs and application resources
│   ├── glue_app_stage.py           # Stage wrapper for multi-account deployment
│   └── config_loader.py            # Configuration utilities
├── job_scripts/                     # Glue job scripts
│   └── process_legislators.py      # Sample ETL job
├── tests/                          # Test suites
├── default-config.yaml             # Configuration file
└── requirements.txt                # Dependencies
```

## Deployment

The application pipeline automatically deploys to:
1. **Dev Environment**: Automatic deployment on code changes
2. **Prod Environment**: Manual approval required

## Jobs

### ProcessLegislators
- **Purpose**: Process US legislators data from public dataset
- **Input**: S3 JSON files from `s3://awsglue-datasets/examples/us-legislators/all`
- **Output**: Processed Parquet files in environment-specific S3 bucket
- **Transformations**: Field selection, timestamp addition, format conversion

## Configuration

Edit `default-config.yaml` to configure:
- Account IDs and regions
- Infrastructure resource references
- Job-specific parameters
- Input/output locations per environment
