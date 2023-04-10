## Connect to Prefect cloud


## Create Prefect blocks
Since the project will primarily use the Google Cloud Platform, Prefect blocks are created using GCP resources. The following are the default block names for this project:
- `bechdel-project-gcp-cred`: block to contain the service account info of the project. This needs a service account file with default location at `~/keys/project_service_key.json`
- `bechdel-project-gcs`: block to make the connection to GCS bucket. Uses key from the credential block.
- `bechdel-project-bq`: block to make the  connection to BigQuery. Uses key from the credential block.

Prefect also reads the scripts from this Github repo and execute it through the agent in the local host:
- `bechdel-project-github`: block to read project's Github repo

See [create_prefect_blocks.py](https://github.com/dherzey/bechdel-movies-project/blob/main/etl/create_prefect_blocks.py) for the Python script to create these Prefect blocks.

## Create Prefect deployments

## Loading from source to Google Cloud Storage

## Loading from Google Cloud Storage to BigQuery