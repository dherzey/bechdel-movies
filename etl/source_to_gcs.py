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
from prefect_gcp.cloud_storage import GcsBucket

import sys
sys.path.append("./scraper")
from scrape_oscars_db import scrape_oscars_data, extract_oscar_results


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

    gcs_block = GcsBucket.load(block_name)
    gcs_block.upload_from_dataframe(df, path, format)


@task(log_prints=True, retries=3, description="Get Oscars data")
def get_oscars_data():
    """
    Uses the two functions found in scrape_oscars_db to
    interact with the Oscars database, extract needed
    elements and format results into a dataframe.
    """

    #interact with database and get page source
    page_source = scrape_oscars_data()
    print("DONE: Scraped Oscars data")

    #extract necessary elements into a dataframe
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
def get_imdb_data(imdb_files: dict):
    """
    Reads movie datasets in chunks from IMDB's site:
    https://datasets.imdbws.com/.

    The following are the datasets that will be loaded:
        - title.basics.tsv.gz
        - title.principals.tsv.gz
        - title.crew.tsv.gz
        - title.ratings.tsv.gz
        - name.basics.tsv.gz

    Arguments:
        imdb_files: dictionary object containing the IMDB 
                    dataset as key (str) and the chunksize 
                    to be followed as its value (int)
                    Example:
                    imdb_files = {
                        'title.basics.tsv.gz': 50_000
                    }
    
    Returns:
        Dataframe of IMDB movie data in chunks
    """

    collection = []
    
    #main loop for reading and loading file
    for filename, chunksize in imdb_files.items():
        url = f'https://datasets.imdbws.com/{filename}'
        
        #read in chunks and add \N as a NULL value
        data = pd.read_csv( url,
                            chunksize=chunksize,
                            iterator=True,
                            sep='\t',
                            header=0,
                            na_values='\\N',
                            encoding='utf-8' )  
        
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
    
    #columns that should not be a string type
    columns = ['tconst', 'isAdult', 'endYear', 'startYear',
               'runtimeMinutes', 'averageRating', 'numVotes', 
               'birthYear', 'deathYear']    
    
    #make sure column dtype is consistent
    for column in columns:
        try:
            if column=='tconst':
                #make unique key as integer for easier merging
                #with the Bechdel dataset
                df[column] = df[column].str\
                                       .replace('tt','')\
                                       .astype(int)    
            else:    
                #if row is str, make row NULL
                df[column] = pd.to_numeric(df[column], 
                                           errors='coerce')
            
            #make sure numeric datatypes are consistent
            #across all data chunks
            if column in ['isAdult', 'birthYear', 
                          'deathYear', 'numVotes']:
                df[column] = df[column].astype('Int64')

            elif column in ['runtimeMinutes', 'averageRating']:
                df[column] = df[column].astype(float)
            
            #convert year columns to datetime
            #note that birthYear and deathYear exceeds the time 
            #range for pd.datetime and throws the following error:
            #pandas._libs.tslibs.np_datetime.OutOfBoundsDatetime: 
            #Out of bounds nanosecond timestamp: 1564-01-01 00:00:00
            if column in ['endYear', 'startYear']:
                df[column] = pd.to_datetime(df[column],
                                            format='%Y')                
        except KeyError:
            pass

    return df


@flow(name="IMDB-data-ingestion")
def imdb_data_flow(block_name):
    """
    Subflow which contains the main IMDB tasks for data
    ingestion and loading to GCS

    Arguments:
        block_name: name of Prefect block for GCS bucket

    Returns:
        None
    """

    imdb_files = {
        'title.basics.tsv.gz': 50_000,
        'title.crew.tsv.gz': 100_000,
        'title.ratings.tsv.gz': 100_000,
        'title.principals.tsv.gz': 200_000,
        'name.basics.tsv.gz': 100_000
    }

    imdb_collection = get_imdb_data(imdb_files)

    #iterate for every IMDB dataset
    for imdb_data, filename in zip(imdb_collection, imdb_files.keys()):       
        filename = "_".join(filename.split(".")[:2])

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


@flow(name="source-to-gcs")
def etl_load_to_gcs(block_name = 'bechdel-project-gcs'):
    """
    Primary workflow for extraction and loading of data.
    All collected data are placed into dataframes that
    will be uploaded to Google Cloud Storage.

    Arguments:
        block_name: name of Prefect block for GCS bucket

    Returns:
        None
    """

    # #get and upload oscars data
    # oscars_data = get_oscars_data()
    # path = Path("oscars/oscars_awards.csv")
    # df_to_gcs(oscars_data, path, 'csv', block_name)

    # #get and upload bechdel test movies data
    # bechdel_data = get_bechdel_data()
    # path = Path("bechdel/bechdel_test_movies.csv")
    # df_to_gcs(bechdel_data, path, 'csv', block_name)

    #get and upload imdb datasets in chunks
    imdb_data_flow(block_name)  


if __name__=="__main__":   
    etl_load_to_gcs()