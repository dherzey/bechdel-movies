"""----------------------------------------------------------------------
Script for creating deployments for Prefect workflows using Python. 
This will look for the script in the Github repo, read it and run it 
through the host machine.

Last modified: April 2023
----------------------------------------------------------------------"""

from prefect import flow
from prefect.deployments import Deployment
from prefect_github import GitHubRepository
from prefect.server.schemas.schedules import CronSchedule

import sys
sys.path.append("./etl")

from source_to_gcs import etl_load_to_gcs
from gcs_to_bigquery import etl_load_to_bq


@flow(name="full-etl-flow")
def etl_full_flow(gcp_block_name = "bechdel-project-gcp-cred", 
                  gcs_block_name = "bechdel-project-gcs",
                  dataset = "data-projects-383009", 
                  bucket_name = "bechdel-project_data-lake"):
    """
    Full ETL workflow which calls both flows for loading data 
    to GCS and to BigQuery

    Arguments:
        - gcp_block_name: Prefect block name for GCP credential
        - gcs_block_name: Prefect block name for GCS bucket
        - dataset: name of the BigQuery dataset
        - bucket_name: name of the GCS bucket where raw the
                       data is stored

    Returns:
        None
    """

    etl_load_to_gcs(gcs_block_name)
    etl_load_to_bq(gcp_block_name, dataset, bucket_name)


def deploy_flow(github_block_name, flow, deploy_name, cron=None):
    """
    Create a Prefect deployment from flow. This uses the
    created Github block to read all scripts to be run.

    Arguments:
        - github_block_name: name of the Github block
        - flow: the flow function to be deployed
        - deploy_name: name of the deployment
        - cron: deployment schedule in cron format.
                Default set to None.

    Returns:
        None
    """

    repo = GitHubRepository.load(github_block_name)

    if cron == None:    
        deploy = Deployment.build_from_flow(
                flow = flow,
                name = deploy_name,
                storage = repo,
        )

    else:
        deploy = Deployment.build_from_flow(
                flow = flow,
                name = deploy_name,
                storage = repo,
                schedule = CronSchedule(cron=cron, timezone="UTC")
        )

    deploy.apply()


if __name__=="__main__":
    github_block_name = "bechdel-project-github"

    # scrape and load all data to GCS and to BigQuery
    # full ETL load runs every 1st day of the month
    deploy_flow(github_block_name, 
                etl_full_flow, 
                "bechdel-etl-full",
                "0 0 1 * *")

    # scrape and load data to GCS (usually for testing)
    # function arguments set to default
    deploy_flow(github_block_name, 
                etl_load_to_gcs, 
                "bechdel-etl-gcs")
    
    # load data from GCS to BigQuery (usually for testing)
    # function arguments set to default
    deploy_flow(github_block_name, 
                etl_load_to_bq, 
                "bechdel-etl-bq")