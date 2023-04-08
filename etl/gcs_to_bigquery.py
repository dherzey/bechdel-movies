from prefect import task, flow
from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage


@task(log_prints=True)
def gcs_to_bigquery(block_name, uri, dataset, table):
    """
    
    """

    gcp_credentials = GcpCredentials.load(block_name)
    
    bigquery_load_cloud_storage(
        uri = uri,
        dataset = dataset,
        table = table,
        gcp_credentials = gcp_credentials
    )


@task()
def gcs_imdb_to_bq():

    return


@flow(name="gcs-to-bigquery")
def etl_load_to_bq(block_name, dataset, bucket_name):
    """
    
    """
    
    # read and load oscars data to bigquery
    uri = f"gs://{bucket_name}/oscars/*.csv"
    gcs_to_bigquery(block_name,
                    uri = uri,
                    dataset = dataset,
                    table = "academy_award")
    
    # read and load Bechdel Test movie list
    uri = f"gs://{bucket_name}/bechdel/*.csv"
    gcs_to_bigquery(block_name,
                    uri = uri,
                    dataset = dataset,
                    table = "bechdel_movies")

    # read and load Bechdel Test movie list
    uri = f"gs://{bucket_name}/bechdel/*.csv"
    gcs_to_bigquery(block_name,
                    uri = uri,
                    dataset = dataset,
                    table = "bechdel_movies")
    

if __name__=="__main__":
    etl_load_to_bq()