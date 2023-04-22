# Bechdel Test in Movies
<i>This project is part of the final requirement for [DataTalks.Club's data engineering bootcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main).</i> 

The Bechdel test, or the Bechdel-Wallace test, is a simple test which measures the representation of women in media. It follows a criteria used to determine how prominent women are in a piece of work. Having all of these criteria passes the Bechdel test:

1. the work has at least two named women
2. the named women talk to each other
3. the named women talk to each other about something besides a man

This project aims to create a data pipeline to ingest data from various sources.

## Data Architecture
![Data architecture of the project!](/diagram/diagram.png)

## Collecting Data From Source
Data is collected from the following web sources and database:
- Academy Awards database
- BechdelTest.com API
- IMDB available datasets
- The Movie Database API (<i>to be added</i>)

See more info in [datasets](https://github.com/dherzey/bechdel-movies-project/blob/main/datasets).

## Configure cloud resources using Terraform
Resources are configured and provisioned using Terraform. This would need GCP service account credentials in order to create a Google Cloud Storage bucket and a BigQuery dataset in the indicated GCP project. See [Terraform folder](https://github.com/dherzey/bechdel-movies-project/blob/main/terraform) for more info.

To run Terraform, make sure to change the path to the service account file and the project's name in the [variables.tf](https://github.com/dherzey/bechdel-movies-project/blob/main/terraform/variables.tf). Then, execute the following commands:
1. `terraform init`
2. `terraform plan`
3. `terraform apply`

## Setting up Prefect flows 
The Python files under the [etl folder](https://github.com/dherzey/bechdel-movies-project/blob/main/etl) contains the scripts for the whole workflow. Using Prefect blocks and flows, we can create Prefect deployments that will run the workflows for the extraction and loading of data to GCS and BigQuery. 

### Create virtual environment
A virtual environment was first created using Python which will contain all necessary packages for deploying through Prefect.

```bash
# install virtualenv
pip install virtualenv

# create virtual environment named project-venv
python3 -m venv project-venv

# activate virtual environment
source ./project-venv/bin/activate

# install all needed packages
pip install -r requirements.txt
```
<i><b>NOTE:</b> For Selenium, an additional webdriver needs to be installed ([install webdriver](https://github.com/dherzey/bechdel-movies-project/blob/main/scraper/README.md)) before running the primary deployment in Prefect. Alternatively, a deployment which does not use Selenium and uses files from [datasets](https://github.com/dherzey/bechdel-movies-project/blob/main/datasets) could be run instead as shown in [running full ETL workflow](https://github.com/dherzey/bechdel-movies-project/tree/main#run-full-etl-workflow).</i>

### Connect to Prefect cloud
We can use Prefect Orion to see our workflows or we can use Prefect cloud. To connect to Prefect cloud, make sure that you have created an account first, then generate your API key through your profile.

```bash
# Make sure Prefect is successfully installed
prefect --help

# (optional) you can create a new profile and set it as your active account
prefect profile create cloud-user
prefect profile use cloud-user

# Login to Prefect cloud. This will prompt for the generated API_KEY
prefect cloud login

# Set configuration for Prefect account
prefect config set PREFECT_API_URL = "<API_URL>"
```

### Create Prefect blocks and deployments
Before running the script to create blocks, make sure the service account file is saved as `~/keys/project_service_key.json`, or change the path to the file under [create_prefect_blocks.py](https://github.com/dherzey/bechdel-movies-project/tree/main/etl/create_prefect_blocks.py). Don't forget to also change the `bucket_name` variable to the appropriate GCS bucket:

```python
if __name__=="__main__":

    # create gcp credentials block
    service_key_path = "~/keys/project_service_key.json" #change service account file path
    gcp_cred_block = create_gcp_cred_block(service_key_path, 
                                           "bechdel-project-gcp-cred")
    
    # create gcs bucket block
    bucket_name = "bechdel-project_data-lake" #change bucket name
    create_gcs_bucket(gcp_cred_block, bucket_name, "bechdel-project-gcs")
```
Additionally, make sure to change the `bucket_name` for the following files: 
- [gcs_to_bigquery.py](https://github.com/dherzey/bechdel-movies-project/tree/main/etl/gcs_to_bigquery.py)
- [flows_to_deploy.py](https://github.com/dherzey/bechdel-movies-project/tree/main/etl/flows_to_deploy.py)

```bash
# create blocks
python3 etl/create_prefect_blocks.py

# create deployments
python3 etl/create_prefect_deployments.py
```

### Run full ETL workflow
Trigger the alternative full ETL deployment to avoid overusing the BechdelTest.com API (as advised by site's owner) or if having trouble with installing Selenium. 

```bash
# start Prefect agent
prefect agent start -q default

# trigger alternative full ETL workflow
prefect deployment run full-etl-flow-alt/bechdel-etl-full-alt
```
It takes approximately 2 hours to run the full script using an `e2-standard-4` instance in GCP.

## Transform data using dbt
Before triggering data transformation of BigQuery tables, make sure to update the service account file path and the project name in [profiles.yml](https://github.com/dherzey/bechdel-movies-project/blob/main/dbt/profiles.yml) for both dev and prod targets. Do the same for the database/project name in [schema.yml](https://github.com/dherzey/bechdel-movies-project/blob/main/dbt/models/staging/schema.yml) under staging models. Then, we can run the following:

```bash
# trigger dbt development for testing
dbt build

# trigger dbt production through Prefect
# this deployment is scheduled to run every month
prefect deployment run dbt-prod-flow/trigger-dbt-prod
```

## Dashboard and data analysis
The dashboard is created using Looker with data connection to BigQuery. View the dashboard [here](https://lookerstudio.google.com/reporting/66b1d9b6-0bf5-4ed3-8a96-50e266f0abef).

## Recommendations
- add other additional analysis and measures, such as whether having more women in the cast/crew affects the Bechdel test score of a movie
- add additional charts in the dashboard and enhance visualization
- further develop and organize dbt models and configurations 
- store variables in a single file for easier update or changes