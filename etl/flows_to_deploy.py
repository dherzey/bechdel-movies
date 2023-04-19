"""----------------------------------------------------------------------
Final set pf Prefect workflows that will serve as the basis for the 
Prefect deployments to be created in create_prefect_deployments.py. This
reads the primary flow functions from the etl and dbt directories.

Last modified: April 2023
----------------------------------------------------------------------"""

import pandas as pd
from pathlib import Path
from prefect import flow

import sys
sys.path.extend(["./etl","./dbt"])

from gcs_to_bigquery import etl_load_to_bq
from trigger_dbt_prefect import trigger_dbt
from source_to_gcs import etl_load_to_gcs, imdb_data_flow, df_to_gcs


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


@flow(name="source-to-gcs-alt")
def etl_load_to_gcs_alt(block_name = 'bechdel-project-gcs'):
    """
    Alternative workflow for extraction and loading of data.
    This uses the Oscars and Bechdel csv files in the project's
    datasets folder (in case of problems in Selenium or issues
    in scraping from their respective sites).

    Arguments:
        block_name: name of Prefect block for GCS bucket

    Returns:
        None
    """

    url = "https://raw.githubusercontent.com/dherzey/bechdel-movies-project/main/datasets"

    # upload oscars data
    from_path = f'{url}/oscars_awards.csv'
    to_path = Path("oscars/oscars_awards.csv")

    df = pd.read_csv(from_path)
    df_to_gcs(df, to_path, 'csv', block_name)

    # upload bechdel test movies data
    from_path = f'{url}/bechdel_test_movies.csv'
    to_path = Path("bechdel/bechdel_test_movies.csv")

    df = pd.read_csv(from_path)
    df_to_gcs(df, to_path, 'csv', block_name)

    # get and upload imdb datasets in chunks
    imdb_data_flow(block_name)  


@flow(name="full-etl-flow-alt")
def etl_full_flow_alt(gcs_block_name = "bechdel-project-gcs",
                  bq_block_name = "bechdel-project-bigquery",
                  dataset = "bechdel_movies_project", 
                  bucket_name = "bechdel-project_data-lake"):
    """
    Alternative full ETL workflow which calls both flows for 
    loading data to GCS and to BigQuery

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

    Arguments:
        - target: the name of the target profile to use. 
                  Can either be dev or prod.
        - is_test: accepts boolean. If False, dbt will give full results
                   of models. Otherwise, results have limited rows.

    Returns:
        None
    """

    trigger_dbt(target, is_test)


@flow(name='dbt-dev-flow')
def trigger_dbt_dev(target='dev', is_test=True):
    """
    Create a flow to trigger dbt commands in development. 
    This uses the prod profile under the dbt folder.

    Arguments:
        - target: the name of the target profile to use. 
                  Can either be dev or prod.
        - is_test: accepts boolean. If False, dbt will give full results
                   of models. Otherwise, results have limited rows.

    Returns:
        None
    """

    trigger_dbt(target, is_test)