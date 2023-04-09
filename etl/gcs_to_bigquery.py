"""----------------------------------------------------------------------
Script for loading data files from GCS to BigQuery using Prefect

Last modified: April 2023
----------------------------------------------------------------------"""

from prefect import task, flow
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect_gcp.bigquery import bigquery_load_cloud_storage


@flow(name="call-bq-load", log_prints=True)
def gcs_to_bigquery(project, uri, dataset, table, location):
    """
    Loads raw data from GCS to BigQuery in an external table.

    Arguments:
        - project: project name where the dataset is located
        - uri: GCS file path, e.g. "gs://bucket-name/..."
        - dataset: name of the BigQuery dataset
        - table: name of the BigQuery table to be created
        - location: dataset location (US, EU, etc.)
    
    Returns:
        Result response of function
    """

    gcp_credentials = GcpCredentials(project=project)
    
    result = bigquery_load_cloud_storage(
                uri = uri,
                dataset = dataset,
                table = table,
                gcp_credentials = gcp_credentials,
                location = location
            )
    
    return result


@flow(name="IMDB-load-BQ", log_prints=True)
def gcs_imdb_to_bq(project, dataset, bucket_name, location):
    """
    Subflow to load IMDB parquet files from GCS to BigQuery

    Arguments:
        - project: project name where the dataset is located
        - dataset: name of the BigQuery dataset
        - bucket_name: name of the GCS bucket where the raw
                       data is stored
        - location: dataset location (US, EU, etc.)
    
    Returns:
        None
    """

    imdb_files = ['title.basics.tsv.gz',
                  'title.principals.tsv.gz',
                  'title.crew.tsv.gz',
                  'title.ratings.tsv.gz']

    # read and load IMDB data
    for filename in imdb_files:
        filename = "_".join(filename.split(".")[:2])
        uri = f"gs://{bucket_name}/imdb/{filename}/*.parquet"
        gcs_to_bigquery(project = project,
                        uri = uri,
                        dataset = dataset,
                        table = f"imdb_{filename}_raw",
                        location = location)
        

@task(log_prints=True)
def bq_tables_partition(dataset, table, column, block_name):
    """
    Creates new table with partitioned columns from external
    raw tables in the dataset.

    Arguments:
        - dataset: name of the BigQuery dataset
        - table: name of the BigQuery table to be created
        - column: name of columns to partition
        - block_name: Prefect block name for BigQuery
    
    Returns:
        BigQueryWarehouse block info
    """

    table_new = table.replace("_raw", "")
    warehouse = BigQueryWarehouse.load(block_name)

    query = f"""
            CREATE OR REPLACE TABLE {dataset}.{table_new}
            PARTITION BY
                {column} AS
            SELECT * FROM {dataset}.{table};
            """

    warehouse.execute(query)

    return warehouse


@flow(name="gcs-to-bigquery")
def etl_load_to_bq(project = "data-project-383009", 
                   dataset = "bechdel_movies_project", 
                   bucket_name = "bechdel-project_data-lake",
                   location = "us-west1"):
    """
    Primary workflow which includes loading data from GCS
    to BigQuery. It also includes creating partitioned
    BigQuery tables from the created external tables.

    Arguments:
        - project: project name where the dataset is located
        - dataset: name of the BigQuery dataset
        - bucket_name: name of the GCS bucket where raw the
                       data is stored
        - location: dataset location (US, EU, etc.)
        - block_name: Prefect block name for BigQuery
    
    Returns:
        None
    """
    
    # read and load oscars data to bigquery
    uri = f"gs://{bucket_name}/oscars/*.csv"
    gcs_to_bigquery(project = project,
                    uri = uri,
                    dataset = dataset,
                    table = "oscars_raw",
                    location = location)
    
    # read and load Bechdel Test movie list
    uri = f"gs://{bucket_name}/bechdel/*.csv"
    gcs_to_bigquery(project = project,
                    uri = uri,
                    dataset = dataset,
                    table = "bechdel_raw",
                    location = location)
    
    # read and load IMDB movie data
    gcs_imdb_to_bq(project, dataset, bucket_name, location)
    

if __name__=="__main__":
    etl_load_to_bq()