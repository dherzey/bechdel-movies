import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True)
def extract_from_gcs(block_name):
    """
    
    """

    gcs_block = GcsBucket.load(block_name)