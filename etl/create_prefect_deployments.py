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
sys.path.extend(["./etl","./dbt"])

from source_to_gcs import etl_load_to_gcs
from gcs_to_bigquery import etl_load_to_bq
from trigger_dbt_prefect import trigger_dbt
from source_to_gcs_alt import etl_load_to_gcs_alt


@flow(name="full-etl-flow")
def etl_full_flow(gcs_block_name = "bechdel-project-gcs",
                  bq_block_name = "bechdel-project-bigquery",
                  dataset = "bechdel_movies_project", 
                  bucket_name = "bechdel-project_data-lake"):
    """
    Full ETL workflow which calls both flows for loading data 
    to GCS and to BigQuery

    Arguments:
        - gcs_block_name: Prefect block name for GCS bucket
        - bq_block_name: Prefect block name for BigQuery
        - dataset: name of the BigQuery dataset
        - bucket_name: name of the GCS bucket where the raw
                       data is stored

    Returns:
        None
    """

    etl_load_to_gcs(gcs_block_name)
    etl_load_to_bq(bq_block_name, dataset, bucket_name)


@flow(name="full-etl-flow-alt")
def etl_full_flow_alt(gcs_block_name = "bechdel-project-gcs",
                  bq_block_name = "bechdel-project-bigquery",
                  dataset = "bechdel_movies_project", 
                  bucket_name = "bechdel-project_data-lake"):
    """
    Full ETL workflow which calls both flows for loading data 
    to GCS and to BigQuery

    Arguments:
        - gcs_block_name: Prefect block name for GCS bucket
        - bq_block_name: Prefect block name for BigQuery
        - dataset: name of the BigQuery dataset
        - bucket_name: name of the GCS bucket where the raw
                       data is stored

    Returns:
        None
    """

    etl_load_to_gcs_alt(gcs_block_name)
    etl_load_to_bq(bq_block_name, dataset, bucket_name)


@flow(name='dbt-prod-flow')
def trigger_dbt_prod(target='prod', is_test=False):
    """
    Create a flow to trigger dbt commands in production. 
    This uses the prod profile under the dbt folder.
    """

    trigger_dbt(target, is_test)


@flow(name='dbt-dev-flow')
def trigger_dbt_dev(target='dev', is_test=True):
    """
    Create a flow to trigger dbt commands in development. 
    This uses the prod profile under the dbt folder.
    """

    trigger_dbt(target, is_test)

    
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
                etl_full_flow_alt, 
                "bechdel-etl-full-alt",
                "0 0 1 * *")

    # scrape and load data to GCS (usually for testing)
    # flow function arguments set to default
    deploy_flow(github_block_name, 
                etl_load_to_gcs, 
                "bechdel-etl-gcs")
    
    # load data from GCS to BigQuery (usually for testing)
    # flow function arguments set to default
    deploy_flow(github_block_name, 
                etl_load_to_bq, 
                "bechdel-etl-bq")
    
    # trigger dbt commands in dev to transform data in BigQuery
    deploy_flow(github_block_name,
                trigger_dbt_dev,
                "trigger-dbt-dev")

    # trigger dbt commands in prod to transform data in BigQuery
    deploy_flow(github_block_name,
                trigger_dbt_prod,
                "trigger-dbt-prod")