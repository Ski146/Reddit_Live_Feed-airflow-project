# Airflow Reddit Live Feed Project

## Overview
- Apache Airflow DAG deployed on an **AWS EC2 instance** to stream Reddit posts live.
- Outputs (processed data) are uploaded to an **AWS S3 bucket** for storage and retrieval.

## Architecture
1. EC2 instance runs Airflow standalone.
2. DAG fetches live Reddit posts using PRAW.
3. Posts are processed and stored in an S3 bucket (`my-bucket-name`).

## How to Run
1. Clone repo
2. Set up Airflow
3. Configure AWS credentials for S3 access
4. Trigger DAG

## OUTPUT IN S3 bucket
<img width="1919" height="1030" alt="image" src="https://github.com/user-attachments/assets/e85b094b-d24b-4215-a204-bd1885609bcb" />
