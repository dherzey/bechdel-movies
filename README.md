# Bechdel movies and the Academy Awards
This project is part of the final requirement for DataTalks.Club's data engineerring bootcamp. 

## Problem Statement


## Data Architecture


## Collecting Data From Source
Data is collected from the following web sources and database:
- Academy Awards database
- BechdelTest.com API
- IMDB available datasets
- The Movie Database API (<i>to be added</i>)

See more info in [datasets](https://github.com/dherzey/bechdel-movies-project/blob/main/datasets)

## Configure cloud resources using Terraform
Resources are configured and provisioned using Terraform. This would need GCP service account credentials in order to create a Google Cloud Storage bucket and a BigQuery dataset in the indicated GCP project. See [Terraform folder](https://github.com/dherzey/bechdel-movies-project/blob/main/terraform) for more info.

## Setting up workflow orchestrator and deployments
The Python files under the [etl folder](https://github.com/dherzey/bechdel-movies-project/blob/main/etl) contains the scripts for the whole workflow. Using Prefect blocks and flows, we can create Prefect deployments that will run the workflows for the extraction and loading of data to GCS and BigQuery. See more info in the [etl folder](https://github.com/dherzey/bechdel-movies-project/blob/main/etl).

## Loading data from source to GCS

## Loading data from storage to BigQuery data warehouse