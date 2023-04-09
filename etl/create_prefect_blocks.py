"""----------------------------------------------------------------------
Script for creating Prefect blocks for the project using Python. Blocks
will contain connection info for cloud resources and repo.

Last modified: April 2023
----------------------------------------------------------------------"""

from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect_github.repository import GitHubRepository


def create_gcp_cred_block(service_key_path, block_name):
    """
    Create a Prefect block to store GCP credentials

    Arguments:
        - service_key_path: local path of the service account
                            key file in json format
        - block_name: the name of this Prefect block

    Returns:
        block name
    """

    credentials_block = GcpCredentials(
        service_account_file = service_key_path
    )
    credentials_block.save(block_name, overwrite=True)
    return block_name


def create_gcs_bucket(gcp_cred_block, bucket_name, block_name):
    """
    Create a Prefect block to connect with GCS bucket

    Arguments:
        - gcp_cred_block: name of the Prefect block storing the
                          the GCP credentials
        - bucket_name: name of the GCS bucket to connect to
        - block_name: the name of this Prefect block

    Returns
        block name
    """

    bucket_block = GcsBucket(
        gcp_credentials = GcpCredentials.load(gcp_cred_block),
        bucket = bucket_name  
    )
    bucket_block.save(block_name, overwrite=True)
    return block_name


def create_bigquery(gcp_cred_block, block_name):

    bq_block = BigQueryWarehouse(
        gcp_credentials = GcpCredentials.load(gcp_cred_block)
    )
    bq_block.save(block_name, overwrite=True)


def create_github_block(repo_url, block_name):
    """
    Create a Prefect block to read contents of a Github repo

    Arguments:
        - repo_url: HTTPS of the Github repo
        - block_name: the name of this Prefect block

    Returns
        block name
    """

    github_block = GitHubRepository(
        repository_url = repo_url
    )
    github_block.save(block_name, overwrite=True)
    return block_name


if __name__=="__main__":

    # create gcp credentials block
    service_key_path = "~/keys/project_service_key.json"
    gcp_cred_block = create_gcp_cred_block(service_key_path, 
                                           "bechdel-project-gcp-cred")
    
    # create gcs bucket block
    bucket_name = "bechdel-project_data-lake"
    create_gcs_bucket(gcp_cred_block, bucket_name, "bechdel-project-gcs")

    # create bigquery block
    create_bigquery(gcp_cred_block, "bechdel-project-bigquery")

    # create github block
    repo_url = "https://github.com/dherzey/bechdel-movies-project.git"
    create_github_block(repo_url, "bechdel-project-github")