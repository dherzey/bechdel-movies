import io
import requests
import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

import sys
sys.path.append("../bechdel-movies-project/scraper")

from scrape_oscars_db import *


@task(log_prints=True, retries=3)
def get_oscars_data():
    """
    Uses the two functions found in scrape_oscars_db to
    interact with the Oscars database, extract needed
    elements and format results into a dataframe.
    """

    page_source = scrape_oscars_data()
    print("DONE: Scraped Oscars data")

    results_df = extract_oscar_results(page_source)
    print("DONE: Extracted needed elements from HTML")
    print("DONE: Data formatted as a structured dataframe\n")

    return results_df


@task(log_prints=True)
def get_bechdel_data():
    """
    Uses the bechdeltest.com API to collect the list of
    movies with and their Bechdel score.

    Arguments: 
        None

    Returns:
        Dataframe of movies and their Bechdel scores
    """

    url = 'http://bechdeltest.com/api/v1/getAllMovies'
    html = requests.get(url).content
    df = pd.read_json(io.StringIO(html.decode('utf-8')))
    
    return df


def get_imdb_data(dataset, chunksize=500_000):
    """
    Reads movie datasets from IMDB's site:
    https://datasets.imdbws.com/.

    The following are some examples of the datasets 
    that can be read:
        - title.basics.tsv.gz
        - title.principals.tsv.gz
        - title.crew.tsv.gz
        - title.ratings.tsv.gz

    Arguments:
        dataset: IMDB dataset to be read
        chunksize: number of rows to read per iteration
    
    Returns:
        Dataframe of IMDB movie data by chunks
    """

    filename = dataset.replace('.tsv.gz','')

    url = f'https://datasets.imdbws.com/{dataset}'

    data = pd.read_csv( url,
                        chunksize=chunksize,
                        iterator=True,
                        sep='\t',
                        header=0 )
    
    return data, filename
            

@task(log_prints=True)
def write_gcs(path, block_name):
    """
    Upload file from path to the indicated bucket
    """
    gcs_block = GcsBucket.load(block_name)
    gcs_block.upload_from_path(from_path=f"{path}",to_path=path)


@flow()
def load_to_gcs():

    return