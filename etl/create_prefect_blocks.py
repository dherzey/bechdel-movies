from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_github.repository import GitHubRepository


def create_gcp_cred_block(service_key_path, block_name):

    credentials_block = GcpCredentials(
        service_account_file = service_key_path
    )
    credentials_block.save(block_name, overwrite=True)
    return block_name


def create_gcp_bucket(gcp_cred_block, bucket_name, block_name):

    bucket_block = GcsBucket(
        gcp_credentials = GcpCredentials.load(gcp_cred_block),
        bucket = bucket_name  
    )
    bucket_block.save(block_name, overwrite=True)
    return block_name


def create_github_block(repo_url, block_name):

    github_block = GitHubRepository(
        repository_url = repo_url
    )
    github_block.save(block_name, overwrite=True)
    return block_name


if __name__=="__main__":

    service_key_path = "../keys/project_service_key.json"
    gcp_cred_block = create_gcp_cred_block(service_key_path, 
                                           "bechdel-project_gcp-cred")
    
    create_gcp_bucket(gcp_cred_block, "")

    repo_url = "https://github.com/dherzey/bechdel-movies-project.git"
    create_github_block(repo_url, "bechdel-project_github")