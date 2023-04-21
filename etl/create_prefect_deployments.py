"""----------------------------------------------------------------------
Script for creating deployments for Prefect workflows using Python. 
This will look for the script in the Github repo, read it and run it 
through the host machine.

Last modified: April 2023
----------------------------------------------------------------------"""

from prefect.deployments import Deployment
from prefect_github import GitHubRepository
from prefect.server.schemas.schedules import CronSchedule

import sys
sys.path.append("./etl")

from flows_to_deploy import *


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

    # alternative ETL flow which uses files from datasets
    # mostly for testing, for issues which occur in Selenium,
    # or to avoid calling the BechdelTest.com API frequently
    deploy_flow(github_block_name, 
                etl_full_flow_alt, 
                "bechdel-etl-full-alt")

    # trigger dbt commands in prod to transform data in BigQuery
    # this will run every 1st day of the month at 4AM
    deploy_flow(github_block_name,
                trigger_dbt_prod,
                "trigger-dbt-prod",
                "0 3 1 * *")