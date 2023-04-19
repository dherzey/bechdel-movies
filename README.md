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

To run Terraform, make sure to change the path of the service account file and the project's name in the [variables.tf](https://github.com/dherzey/bechdel-movies-project/blob/main/terraform/variables.tf). Then, we can execute the following commands:
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
<i><b>NOTE:</b> For Selenium, an additional webdriver needs to be installed. [See more info](https://github.com/dherzey/bechdel-movies-project/blob/main/scraper/README.md).</i>

### Create Prefect blocks and deployments
```bash
# create blocks
python3 etl/create_prefect_blocks.py

# create deployments
python3 etl/create_prefect_deployments.py
```

## Transform data using dbt

## Final dashboard