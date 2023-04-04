"""----------------------------------------------------------------------
Script for collecting movie data and information from the Bechdel test 
website using their API and from IMDB using their available datasets.

Last modified: March 2023
----------------------------------------------------------------------"""

import io
import requests
import pandas as pd


def get_bechdel_list():
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


def get_imdb_data(chunksize=500_000):
    """
    Reads movie datasets from IMDB

    Arguments:
        chunksize: number of rows to read per iteration
    
    Returns:
        Dataframe of IMDB movie data
    """

    url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
    imdb_titles = pd.read_csv(url,
                              chunksize=chunksize,
                              iterator=True,
                              sep='\t',
                              header=0)
    
    return imdb_titles


if __name__=="__main__":

    get_bechdel_list()