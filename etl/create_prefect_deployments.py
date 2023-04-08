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


def deploy_flow(github_block_name, flow):
    """
    Create a Prefect deployment from flow. This uses the
    created Github block to read all scripts to be run.

    Arguments:
        - github_block_name: name of the Github block
        - flow: the flow function to be deployed

    Returns:
        None
    """

    repo = GitHubRepository.load(github_block_name)

    Deployment.build_from_flow(
        flow = flow,
        name = f"{flow}-deploy",
        storage = repo
    )


if __name__=="__main__":

    github_block_name = "bechdel_project-github"
    deploy_flow(github_block_name, etl_load_to_gcs)