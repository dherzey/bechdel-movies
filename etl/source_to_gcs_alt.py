"""----------------------------------------------------------------------
Alternative script of source_to_gcs.py for loading dataframes from 
the source to Google Cloud Storage. 

Last modified: April 2023
----------------------------------------------------------------------"""

from pathlib import Path
from prefect import flow
from source_to_gcs import imdb_data_flow
from prefect_gcp.cloud_storage import GcsBucket


@flow(name="source-to-gcs-alt")
def etl_load_to_gcs_alt(block_name = 'bechdel-project-gcs'):
    """
    Alternative workflow for extraction and loading of data.
    This uses the saved Oscars and Bechdel csv files in the
    datasets folder (in case of problems in Selenium or issues
    in scraping from their respective sites). All collected data 
    will be uploaded to Google Cloud Storage.

    Arguments:
        block_name: name of Prefect block for GCS bucket

    Returns:
        None
    """

    gcs_block = GcsBucket.load(block_name)

    # upload oscars data
    from_path = Path("./datasets/oscars_awards.csv")
    to_path = Path("oscars/oscars_awards.csv")
    gcs_block.upload_from_path(from_path, to_path)

    # upload bechdel test movies data
    from_path = Path("./datasets/bechdel_test_movies.csv")
    to_path = Path("oscars/bechdel_test_movies.csv")
    gcs_block.upload_from_path(from_path, to_path)

    # get and upload imdb datasets in chunks
    imdb_data_flow(block_name)  


if __name__=="__main__":   
    etl_load_to_gcs_alt()