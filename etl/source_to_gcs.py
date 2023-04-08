"""----------------------------------------------------------------------
Script for loading dataframes from the source to Google Cloud Storage

Last modified: April 2023
----------------------------------------------------------------------"""

import io
import time
import requests
import pandas as pd
from pathlib import Path
from prefect import task, flow
from google.cloud import storage
from prefect_gcp.cloud_storage import GcsBucket

import sys
sys.path.append("../bechdel-movies-project/scraper")

from scrape_oscars_db import *


@task(log_prints=True, description="Upload dataframe to GCS")
def df_to_gcs(df, path, format, block_name):
    """
    Upload dataframe to the storage bucket

    Arguments:
        - df: dataframe to be uploaded
        - path: storage bucket path to upload file
        - format: file format of the dataframe
        - block_name: name of the Prefect block for gcs
    """

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    gcs_block = GcsBucket.load(block_name)
    gcs_block.upload_from_dataframe(df, path, format)


@task(log_prints=True, retries=3, description="Get Oscars data")
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

    return results_df


@task(log_prints=True, description="Get Bechdel data")
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


@task(log_prints=True, description="Get IMDB data")
def get_imdb_data(datasets, chunksize):
    """
    Reads movie datasets in chunks from IMDB's site:
    https://datasets.imdbws.com/.

    The following are some examples of the datasets 
    that can be read:
        - title.basics.tsv.gz
        - title.principals.tsv.gz
        - title.crew.tsv.gz
        - title.ratings.tsv.gz

    Arguments:
        dataset: a list of IMDB datasets to be read
        chunksize: number of rows to read per iteration.
    
    Returns:
        Dataframe of IMDB movie data in chunks
    """

    collection = []
    
    for dataset in datasets:

        url = f'https://datasets.imdbws.com/{dataset}'

        data = pd.read_csv( url,
                            chunksize=chunksize,
                            iterator=True,
                            sep='\t',
                            header=0 )  
        
        collection.append(data) 

    return collection  


@task(log_prints=True, description="Transform dataframe")
def transform_imdb_data(df):
    """
    Transform the format or change the datatype of
    some columns in the IMDB dataframe

    Arguments:
        df: the IMDB dataframe

    Returns:
        Transformed IMDB dataframe
    """
    
    df['tconst'] = df['tconst'].str\
                               .replace('tt','')\
                               .astype(int)
    
    columns = ['isAdult',
               'endYear',
               'startYear',
               'runtimeMinutes']
    
    for column in columns:
        try:
            df[column] = pd.to_numeric(df[column], errors='coerce')
        except KeyError:
            pass
    
    return df


@flow(name="IMDB ingestion")
def imdb_data_flow(block_name, chunksize=50_000):
    """
    Subflow which contain the main IMDB tasks

    Arguments:
        - block_name: name of Prefect block for GCS bucket
        - chunksize: number of rows to read per iteration.
                     Default set to 50,000 rows.

    Returns:
        None
    """

    datasets = ['title.basics.tsv.gz',
                'title.principals.tsv.gz',
                'title.crew.tsv.gz',
                'title.ratings.tsv.gz']

    imdb_collection = get_imdb_data(datasets, chunksize)

    for imdb_data, dataset in zip(imdb_collection, datasets):
        
        filename = dataset.replace('.tsv.gz','').replace('.','_')

        count = 0
        while True:
            try:
                count += 1                
                chunk_data = next(imdb_data)
                chunk_data = transform_imdb_data(chunk_data)

                #load dataframe to GCS
                main = f"imdb/{filename}/{filename}"
                path = Path(f"{main}_part{count:02}.parquet")
                df_to_gcs(chunk_data, path, 'parquet', block_name)   

                #delay next iteration
                time.sleep(5)

            except StopIteration:
                break


@flow(name="Source to GCS")
def etl_load_to_gcs(block_name):
    """
    Primary workflow for extraction and loading of data.
    All collected data are placed into dataframes that
    will be uploaded to Google Cloud Storage.

    Arguments:
        block_name: name of Prefect block for GCS bucket

    Returns:
        None
    """

    #get and upload oscars data
    oscars_data = get_oscars_data()
    path = Path("oscars/oscars_awards.csv")
    df_to_gcs(oscars_data, path, 'csv', block_name)

    #get and upload bechdel test movies data
    bechdel_data = get_bechdel_data()
    path = Path("bechdel/bechdel_test_movies.csv")
    df_to_gcs(bechdel_data, path, 'csv', block_name)

    #get and upload imdb datasets in chunks
    imdb_data_flow(block_name)  


if __name__=="__main__":   
    etl_load_to_gcs('bechdel-project-gcs')