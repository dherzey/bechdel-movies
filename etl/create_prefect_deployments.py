"""----------------------------------------------------------------------
Script for creating deployments for Prefect workflows using Python. 
This will look for the script in the Github repo, read it and run it 
through the host machine.

Last modified: April 2023
----------------------------------------------------------------------"""

import os
import sys
from prefect.deployments import Deployment
from prefect_github import GitHubRepository
from prefect.server.schemas.schedules import CronSchedule

# path = os.path.join(os.getcwd(), "etl")
# sys.path.extend([path, os.getcwd()])
sys.path.insert(0, os.getcwd())

from etl.source_to_gcs import etl_load_to_gcs
from etl.gcs_to_bigquery import etl_load_to_bq


def deploy_flow(github_block_name, flow, deploy_name, cron):
    """
    Create a Prefect deployment from flow. This uses the
    created Github block to read all scripts to be run.

    Arguments:
        - github_block_name: name of the Github block
        - flow: the flow function to be deployed
        - deploy_name: name of the deployment
        - cron: deployment schedule in cron format

    Returns:
        None
    """

    repo = GitHubRepository.load(github_block_name)
    
    deploy = Deployment.build_from_flow(
        flow = flow,
        name = deploy_name,
        storage = repo,
        schedule = CronSchedule(cron=cron, timezone="UTC")
    )

    deploy.apply()


if __name__=="__main__":

    github_block_name = "bechdel-project-github"

    # loading to GCS will run every first day of the month
    deploy_flow(github_block_name, 
                etl_load_to_gcs, 
                "bechdel-etl-gcs-dep",
                "0 0 1 * *")
    
    # loading to GCS will run every second day of the month
    deploy_flow(github_block_name, 
                etl_load_to_bq, 
                "bechdel-etl-bq-dep",
                "0 0 2 * *")