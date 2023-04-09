from prefect import task, flow
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect_gcp.bigquery import bigquery_load_cloud_storage


@flow(name="call-bq-load", log_prints=True)
def gcs_to_bigquery(block_name, uri, dataset, table):
    """
    Loads raw data from GCS to BigQuery in an external table.

    Arguments:
        - block_name: Prefect block name for GCP credential
        - uri: GCS file path
        - dataset: name of the BigQuery dataset
        - table: name of the BigQuery table to be created
    
    Returns:
        None
    """

    gcp_credentials = GcpCredentials.load(block_name)
    
    bigquery_load_cloud_storage(
        uri = uri,
        dataset = dataset,
        table = table,
        gcp_credentials = gcp_credentials
    )


@flow(name="IMDB-load-BQ", log_prints=True)
def gcs_imdb_to_bq(block_name, dataset, bucket_name):
    """
    Subflow to load IMDB parquet files from GCS to BigQuery

    Arguments:
        - block_name: Prefect block name for GCP credential
        - dataset: name of the BigQuery dataset
        - bucket_name: name of the GCS bucket where raw the
                       data is stored
    
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
        gcs_to_bigquery(block_name,
                        uri = uri,
                        dataset = dataset,
                        table = f"imdb_{filename}_raw")

@task(log_prints=True)
def bq_tables_partition(dataset, table, column, block_name):
    """
    Creates new table with partitioned columns from external
    raw tables in the dataset.
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
def etl_load_to_bq(block_name = "bechdel-project-gcp-cred", 
                   dataset = "bechdel_movies_project", 
                   bucket_name = "bechdel-project_data-lake"):
    """
    Primary workflow which includes loading data from GCS
    to BigQuery. It also includes creating partitioned
    BigQuery tables from the created external tables.

    Arguments:
        - block_name: Prefect block name for GCP credential
        - dataset: name of the BigQuery dataset
        - bucket_name: name of the GCS bucket where raw the
                       data is stored
    
    Returns:
        None
    """
    
    # read and load oscars data to bigquery
    uri = f"gs://{bucket_name}/oscars/*.csv"
    gcs_to_bigquery(block_name,
                    uri = uri,
                    dataset = dataset,
                    table = "oscars_raw")
    
    # read and load Bechdel Test movie list
    uri = f"gs://{bucket_name}/bechdel/*.csv"
    gcs_to_bigquery(block_name,
                    uri = uri,
                    dataset = dataset,
                    table = "bechdel_raw")
    
    # read and load IMDB movie data
    gcs_imdb_to_bq(block_name, dataset, bucket_name)
    

if __name__=="__main__":
    etl_load_to_bq()