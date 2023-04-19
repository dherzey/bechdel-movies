## Create virtual environment
A virtual environment was first created using Python to contain all necessary packages for deploying through Prefect along with other packages for scraping and modelling data. See [requirements.txt](https://github.com/dherzey/bechdel-movies-project/blob/main/requirements.txt) in the main directory for the full list of packages.

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

## Connect to Prefect cloud
We can use the local Prefect Orion to see our workflows or we can use Prefect cloud (which I prefer since it have extra features such as automation). To connect to Prefect cloud, make sure that you have created an account first.
```bash
# make sure Prefect is successfully installed
prefect --help

# (optional) you can create a new profile and set it as your active account
prefect profile create cloud-user
prefect profile use cloud-user

# login to Prefect cloud. This will prompt for an API_KEY which can be generated
prefect cloud login

# set configuration for Prefect account
prefect config set PREFECT_API_URL = "<API_URL>"
```

## Create Prefect blocks
Since the project will primarily use the Google Cloud Platform, Prefect blocks are created using GCP resources. The following are the default block names for this project:
- `bechdel-project-gcp-cred`: block to contain the service account info of the project. This needs a service account file with default location at `~/keys/project_service_key.json`
- `bechdel-project-gcs`: block to make the connection to GCS bucket. Uses key from the credential block.
- `bechdel-project-bq`: block to make the  connection to BigQuery. Uses key from the credential block.

Prefect also reads the scripts from a Github repo and execute it through the Prefect agent in the local host:
- `bechdel-project-github`: block with the HTTPS info of the Github repo where all the scripts are located. The default is set to this repository (https://github.com/dherzey/bechdel-movies-project.git)

See [create_prefect_blocks.py](https://github.com/dherzey/bechdel-movies-project/blob/main/etl/create_prefect_blocks.py) for the Python script to create these Prefect blocks.

## Create Prefect deployments

## Loading from source to Google Cloud Storage

## Loading from Google Cloud Storage to BigQuery