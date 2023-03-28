"""----------------------------------------------------------------------
Script for collecting movie data and datasets from online source and API
Last modified: March 2023
----------------------------------------------------------------------"""

import io
import requests
import pandas as pd
from datetime import date


def get_bechdel_list():

    url = 'http://bechdeltest.com/api/v1/getAllMovies'
    html = requests.get(url).content
    df = pd.read_json(io.StringIO(html.decode('utf-8')))
    
    return df


def get_imdb_data(chunksize=500_000):

    url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
    imdb_titles = pd.read_csv(url,
                              chunksize=chunksize,
                              iterator=True,
                              sep='\t',
                              header=0)
    
    return imdb_titles


def get_tmdb_data(API_KEY):

    #get latest year
    latest = date.today().year

    #dictionary to contain year and number of pages
    total_pages = {}

    #gather all the pages per year
    url = f'https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}'
    for year in range(1874, latest):
        response = requests.get(f'{url}&primary_release_year={year}')
        movies = response.json()
        pages[year] = movies['total_pages']

    #dictionary to contain year and tmdb ids
    tmdb_ids = {}

    #collect top 500 most and least popular tmdb ids per year
    for year, pages in total_pages.items():
        for page in pages:
            response = requests.get(f'{url}&primary_release_year={year}&page={page}')


    return 