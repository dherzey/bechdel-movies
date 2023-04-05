"""----------------------------------------------------------------------
Script for collecting movie data and information from the Bechdel test 
website using their API and from IMDB using their available datasets.

Last modified: March 2023
----------------------------------------------------------------------"""

import io
import requests
import pandas as pd


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


def get_imdb_data(chunksize=500_000):
    """
    Reads movie datasets from IMDB. The following are 
    the datasets to be read:
        - title.basics.tsv.gz
        - title.principals.tsv.gz
        - title.crew.tsv.gz
        - title.ratings.tsv.gz

    Arguments:
        chunksize: number of rows to read per iteration
    
    Returns:
        Dataframe of IMDB movie data
    """

    datasets = ['title.basics.tsv.gz',
                'title.principals.tsv.gz',
                'title.crew.tsv.gz',
                'title.ratings.tsv.gz']

    for dataset in datasets:
        filename = dataset.replace('.tsv.gz','')

        count = 0
        while True:
            try:
                count += 1
                url = f'https://datasets.imdbws.com/{dataset}'

                data = pd.read_csv( url,
                                    chunksize=chunksize,
                                    iterator=True,
                                    sep='\t',
                                    header=0 )
                
                data_part = next(data)

                #change format and data type of tconst
                data_part['tconst'] = data_part['tconst'].str\
                                                            .replace('tt','')\
                                                            .astype(int)

                data_part.to_csv(f"{filename}_part{count}.csv",
                                    index=False)
            
            except StopIteration:
                break