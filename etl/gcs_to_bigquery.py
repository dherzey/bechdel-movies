"""----------------------------------------------------------------------
Script for loading data files from GCS to BigQuery using Prefect

Last modified: April 2023
----------------------------------------------------------------------"""

from prefect import task, flow
from prefect_gcp.bigquery import BigQueryWarehouse


@task(log_prints=True)
def gcs_to_bigquery(block_name, dataset, table, uri, format):
    """
    Loads raw data from GCS to BigQuery in an external table.
    This executes a SQL query in the data warehouse.

    Arguments:
        - block_name: Prefect block name for BigQuery
        - dataset: name of the BigQuery dataset
        - table: name of the BigQuery table to be created
        - uri: GCS file path, e.g. "gs://bucket-name/..."
        - format: file format of the files to be read
    
    Returns:
        BigQuery block info
    """

    warehouse = BigQueryWarehouse.load(block_name)

    query = f"""
            CREATE OR REPLACE EXTERNAL TABLE {dataset}.{table}
            OPTIONS (
                format = {format},
                uris = {uri}
            );
            """
    
    #execute changes in BigQuery
    warehouse.execute(query)
    return warehouse


@flow(name="IMDB-load-BQ", log_prints=True)
def gcs_imdb_to_bq(block_name, bucket_name, dataset, format):
    """
    Subflow to load IMDB parquet files from GCS to BigQuery

    Arguments:
        - block_name: Prefect block name for BigQuery
        - bucket_name: GCS bucket name where data is stored
        - dataset: name of the BigQuery dataset
        - format: file format of the files to be read
    
    Returns:
        None
    """

    imdb_files = ['title.basics.tsv.gz',
                  'title.principals.tsv.gz',
                  'title.crew.tsv.gz',
                  'title.ratings.tsv.gz',
                  'name.basics.tsv.gz']

    for filename in imdb_files:
        filename = "_".join(filename.split(".")[:2])
        uri = [f"gs://{bucket_name}/imdb/{filename}/*.{format}"]

        #load to BigQuery
        gcs_to_bigquery(block_name = block_name,
                        dataset = dataset,
                        table = f"imdb_{filename}",
                        uri = uri,
                        format = format)
        

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

    warehouse = BigQueryWarehouse.load(block_name)

    #create new table with partition
    query = f"""
            CREATE OR REPLACE TABLE {dataset}.{table}_partitioned
            PARTITION BY
                DATE({column}) AS
            SELECT * FROM {dataset}.{table};
            """

    #execute changes in BigQuery
    warehouse.execute(query)
    return warehouse


@flow(name="gcs-to-bigquery")
def etl_load_to_bq(block_name = "bechdel-project-bigquery", 
                   dataset = "bechdel_movies_project", 
                   bucket_name = "bechdel-project_data-lake"):
    """
    Primary workflow which includes loading data from GCS
    to BigQuery. It also includes creating partitioned
    BigQuery tables from the created external tables.

    Arguments:
        - block_name: Prefect block name for BigQuery
        - dataset: name of the BigQuery dataset
        - bucket_name: name of the GCS bucket where raw the
                       data is stored

    Returns:
        None
    """
    
    # read and load oscars data to bigquery
    uri = [f"gs://{bucket_name}/oscars/*.csv"]
    gcs_to_bigquery(block_name = block_name,
                    dataset = dataset,
                    table = "oscars",
                    uri = uri,
                    format = "csv")
    
    # read and load Bechdel Test movie list
    uri = [f"gs://{bucket_name}/bechdel/*.csv"]
    gcs_to_bigquery(block_name = block_name,
                    dataset = dataset,
                    table = "bechdel",
                    uri = uri,
                    format = "csv")
    
    # read and load IMDB movie data
    gcs_imdb_to_bq(block_name = block_name, 
                   bucket_name =  bucket_name, 
                   dataset = dataset, 
                   format = "parquet")

    """---------------------------------------------"""

    #create new table with partition for imdb data
    bq_tables_partition(dataset, "imdb_title_basics", "startYear", block_name)

    #create new table with partition for oscars data
    # bq_tables_partition(dataset, "bechdel_raw", "year", block_name)
    

if __name__=="__main__":
    etl_load_to_bq()