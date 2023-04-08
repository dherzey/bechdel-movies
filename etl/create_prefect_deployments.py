"""----------------------------------------------------------------------
Script for creating deployments for Prefect workflows using Python. 
This will look for the script in the Github repo, read it and run it 
through the host machine.

Last modified: April 2023
----------------------------------------------------------------------"""

from prefect.deployments import Deployment
from prefect_github import GitHubRepository

import sys
sys.path.append("../bechdel-movies-project/etl")

from source_to_gcs import etl_load_to_gcs


def deploy_flow(github_block_name, flow, deploy_name):
    """
    Create a Prefect deployment from flow. This uses the
    created Github block to read all scripts to be run.

    Arguments:
        - github_block_name: name of the Github block
        - flow: the flow function to be deployed
        - deploy_name: name of the deployment

    Returns:
        None
    """

    repo = GitHubRepository.load(github_block_name)

    deploy = Deployment.build_from_flow(
        flow = flow,
        name = deploy_name,
        storage = repo
    )

    deploy.apply()


if __name__=="__main__":

    github_block_name = "bechdel-project-github"

    deploy_flow(github_block_name, 
                etl_load_to_gcs, 
                "bechdel-etl-gcs-dep")