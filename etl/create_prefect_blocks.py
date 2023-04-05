from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


credentials_block = GcpCredentials(
    service_account_file = '/home/jdtganding/Documents/data-engineering-zoomcamp/service_account_key.json'
)
credentials_block.save("bechdel_project-gcp_creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials = GcpCredentials.load("bechdel-gcp-creds"),
    bucket = "bechdel_project_data_storage",  
)

bucket_block.save("bechdel_project-gcs", overwrite=True)


from prefect_github.repository import GitHubRepository

github_block = GitHubRepository(
    repository_url="https://github.com/dherzey/DataTalks_DataEngineering_2023.git"
)

github_block.save("bechdel_project-github", overwrite=True)