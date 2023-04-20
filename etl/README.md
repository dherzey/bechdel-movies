## Prefect blocks
Since the project will primarily use the Google Cloud Platform, Prefect blocks are created using GCP resources. The following are the default block names for this project:
- `bechdel-project-gcp-cred`: block to contain the service account info of the project. This needs a service account file with default location at `~/keys/project_service_key.json`
- `bechdel-project-gcs`: block to make the connection to GCS bucket. Uses key from the credential block.
- `bechdel-project-bq`: block to make the  connection to BigQuery. Uses key from the credential block.

Prefect also reads the scripts from a Github repo and executes it through the Prefect agent in the local host:
- `bechdel-project-github`: block with the HTTPS info of the Github repo where all the scripts are located. The default is set to this repository (https://github.com/dherzey/bechdel-movies-project.git)

## Prefect deployments
The following workflows are deployed through Prefect:
- `bechdel-etl-full`: 
- `bechdel-etl-full-alt`:
- `trigger-dbt-prod`:

## Loading from source to Google Cloud Storage
The primary Python script for ingesting data from source to GCS is found in [source_to_gcs.py](https://github.com/dherzey/bechdel-movies-project/blob/main/etl/source_to_gcs.py). This starts by calling the Oscar scraper functions to interatc and collect data from the Oscars' database. 

## Loading from Google Cloud Storage to BigQuery