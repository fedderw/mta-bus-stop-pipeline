# mta-bus-stop-pipeline

## Overview
This Python script is designed to automate the process of downloading, transforming, and uploading MTA bus stops data to an AWS S3 bucket. It uses various libraries such as `pandas`, `geopandas`, and `prefect` to handle data operations and flow management.

## Dependencies
- argparse
- calendar
- datetime
- re
- subprocess
- boto3
- geopandas
- pandas
- requests
- prefect

## Setup
1. Ensure all dependencies are installed.
2. Configure AWS credentials to access S3.

## Usage
Run the script with the following command:

```bash
python <script_name>.py --s3-bucket <your_s3_bucket_name> --s3-key <your_s3_key>
```
