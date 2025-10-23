# AWS Data Pipeline Applications

This repository contains the application layer for AWS Glue data processing pipelines.

## 🏗️ Architecture

This repository is part of a dual-pipeline architecture:

- **Infrastructure Repository**: Deploys S3 buckets, IAM roles, Glue databases, EventBridge rules
- **Application Repository** (this repo): Deploys Glue jobs and data processing logic

## 🚀 Quick Start

**⚠️ IMPORTANT**: Before using this repository, you must customize it with your own AWS account numbers and GitHub connection.

### 1. Follow the Bootstrap Guide

This repository requires initial setup and configuration. Please follow the comprehensive setup guide:

**📖 [Developer Bootstrap Guide](../aws-glue-pipeline-infrastructure/docs/DEVELOPER_BOOTSTRAP_GUIDE.md)** (located in the infrastructure repository)

The bootstrap guide will walk you through:

- Setting up your local environment
- Configuring AWS accounts and credentials
- Creating GitHub connections
- Updating configuration files
- Deploying the applications

### 2. Required Customizations

Before deploying, you must update these files with your information:

#### `default-config.yaml`

```yaml
pipelineAccount:
  awsAccountId: YOUR_PIPELINE_ACCOUNT_ID  # Replace this
  awsRegion: YOUR_PREFERRED_REGION        # Replace this
```

#### `aws_glue_applications/app_pipeline_stack.py`

```python
GITHUB_REPO = "YOUR_GITHUB_USERNAME/YOUR_APPLICATION_REPO_NAME"  # Replace this
GITHUB_CONNECTION_ARN = "arn:aws:codeconnections:YOUR_REGION:YOUR_PIPELINE_ACCOUNT:connection/YOUR_CONNECTION_ID"  # Replace this
```

## 📁 Repository Structure

```bash
aws-data-pipeline-applications/
├── app.py                           # CDK app entry point
├── aws_glue_applications/           # Python package
│   ├── app_pipeline_stack.py        # CI/CD pipeline for applications
│   ├── glue_app_stack.py           # Glue jobs and application resources
│   ├── glue_app_stage.py           # Stage wrapper for multi-account deployment
│   └── config_loader.py            # Configuration utilities
├── job_scripts/                     # Glue job scripts
│   ├── file_processor.py           # CSV to Parquet processor
│   └── process_legislators.py      # Sample ETL job
├── tests/                          # Test suites
├── default-config.yaml             # Configuration file (customize this)
└── requirements.txt                # Dependencies
```

## 🔄 CI/CD Pipeline

The application pipeline automatically:

1. **Source**: Pulls code from your GitHub repository
2. **Build**: Runs CDK synth and tests
3. **Deploy Dev**: Deploys Glue jobs to development account
4. **Deploy Prod**: Deploys to production (with manual approval)

## 📊 Glue Jobs

### FileProcessor

- **Purpose**: Event-driven CSV to Parquet conversion
- **Trigger**: Automatic when CSV files are uploaded to S3 input bucket
- **Input**: CSV files in `s3://glue-input-dev-{ACCOUNT}/`
- **Output**: Parquet files in `s3://glue-output-dev-{ACCOUNT}/`
- **Features**: Schema inference, metadata enrichment, format optimization

### ProcessLegislators

- **Purpose**: Process US legislators data from public dataset
- **Input**: S3 JSON files from `s3://awsglue-datasets/examples/us-legislators/all`
- **Output**: Processed Parquet files in environment-specific S3 bucket
- **Transformations**: Field selection, timestamp addition, format conversion

## 🔧 Local Development

### Prerequisites

- Python 3.9+
- AWS CLI configured
- AWS CDK v2
- Node.js (for CDK)

### Setup

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install CDK
npm install -g aws-cdk
```

### Deploy

```bash
# Deploy applications (after infrastructure is deployed)
cdk deploy --profile YOUR_PIPELINE_PROFILE
```

## 🧪 Testing

### End-to-End Testing

```bash
# Upload test CSV file
aws s3 cp test_data.csv s3://glue-input-dev-{YOUR_DEV_ACCOUNT}/ --profile dev-{region}

# Monitor job execution
aws glue get-job-runs --job-name "FileProcessorV2-dev" --profile dev-{region}

# Check output
aws s3 ls s3://glue-output-dev-{YOUR_DEV_ACCOUNT}/ --profile dev-{region} --recursive
```

### Unit Testing

```bash
# Run unit tests
python -m pytest tests/unit/

# Run integration tests
python -m pytest tests/integration/
```

## 📈 Data Flow

1. **CSV Upload**: Files uploaded to S3 input bucket
2. **Event Trigger**: S3 event notification triggers Lambda
3. **Job Start**: Lambda starts appropriate Glue job
4. **Processing**: Glue job processes data (CSV → Parquet)
5. **Output**: Processed files saved to S3 output bucket
6. **Metadata**: Processing metadata added automatically

## 🔐 Security

- Uses IAM service roles for Glue job execution
- Cross-account deployment with least-privilege access
- Secure handling of data in transit and at rest

## 📊 Monitoring

- CloudWatch logs for all Glue jobs
- Job execution metrics and monitoring
- Error tracking and alerting

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add or modify Glue jobs in `job_scripts/`
4. Update tests as needed
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For setup help, see the [Developer Bootstrap Guide](../aws-glue-pipeline-infrastructure/docs/DEVELOPER_BOOTSTRAP_GUIDE.md) in the infrastructure repository.

For issues or questions, please open a GitHub issue.