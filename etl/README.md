## Connect to Prefect cloud


## Create Prefect blocks
Since the project will primarily use the Google Cloud Platform, Prefect blocks are created with this in mind:
- Google Cloud Credentials: block to contain the service account info of the project
- Google Cloud Storage Bucket: bloc to make connection to GCS bucket
- Google Cloud BigQuery: block to make connection to BigQuery

This Github repo is also used to create a block to read all needed scripts:
- GitHub Repository: block to read from GitHub Repo

## Loading from source to Google Cloud Storage

## Loading from storage to BigQuery