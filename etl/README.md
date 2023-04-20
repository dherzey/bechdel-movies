## Prefect blocks
Since the project will primarily use the Google Cloud Platform, Prefect blocks are created using GCP resources. The following are the default block names for this project:
- `bechdel-project-gcp-cred`: block to contain the service account info of the project. This needs a service account file with default location at `~/keys/project_service_key.json`
- `bechdel-project-gcs`: block to make the connection to GCS bucket. Uses key from the credential block.
- `bechdel-project-bq`: block to make the  connection to BigQuery. Uses key from the credential block.

Prefect also reads the scripts from a Github repo and executes it through the Prefect agent in the local host:
- `bechdel-project-github`: block with the HTTPS info of the Github repo where all the scripts are located. The default is set to this repository (https://github.com/dherzey/bechdel-movies-project.git)

## Prefect deployments
The following are the primary pipelines deployed through Prefect:
- `full-etl-flow/bechdel-etl-full`: full ETL workflow which spans ingestion of data from the source (which includes web scraping and reading data by chunks), loading of raw data to GCS bucket in csv and parquet formats, and finally, creating the needed tables in BigQuery with partition and clustering.
- `dbt-prod-flow/trigger-dbt-prod`: this workflow triggers the `dbt build` command for the production target profile. This reads the tables from BigQuery and transform them into new tables and views needed for further data analysis.