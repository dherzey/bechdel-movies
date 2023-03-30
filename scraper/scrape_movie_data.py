"""----------------------------------------------------------------------
Script for collecting movie data and datasets from online source and API
Last modified: March 2023
----------------------------------------------------------------------"""

import io
import time
import requests
import pandas as pd
from datetime import date
from configparser import ConfigParser


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


def response_json(API_URL):
    """
    Get content of specified url

    Arguments:
        API_URL: url of API

    Returns:
        json object of the response
    """

    response = requests.get(API_URL)

    if response.status_code==200:
        return response.json()
    else:
        return "REQUEST ERROR"


def get_tmdb_data(API_KEY, from_year=1874, to_year=None, delay=5):
    """
    Scrapes and extract movie credits data from The Movie Database 
    using heir own API.

    Arguments:
        - API_KEY: the generated API key from the tmdb API
        - from_year: beginning year from when we start our scraping.
                     Default set to 1874. 
        - to_year: final year from when we end our scraping.
                   Default set to None which will get current year.
        - delay: time delay in seconds in-between requests. 
                 Default set to 5 seconds.

    Returns:
        Dataframe with the following columns:
            - tmdb_id: movie id from tmdb
            - imdb_id: movie id from imdb
            - tmdb_person_id: tmdb id of cast/crew
            - name: person name of cast/crew
            - gender: person's gender (0-unspecified,
                      1-female, 2-male)
            - department: film department the person
                          is known for
            - job: person job within the department
            - credit_type: whether person is cast or crew
            - imdb_person_id: imdb id of cast/crew
    """

    #get latest year
    if to_year == None:
        latest = date.today().year
    else:
        latest = to_year

    #main discover api url
    url = f'https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}'

    #dictionary to contain total number of pages
    total_pages = {}

    # (1) collect total number of pages per year
    print("START: Collecting total number of pages per year...")

    for year in range(from_year, latest+1):
        movies = response_json(f'{url}&primary_release_year={year}')
        pages = movies['total_pages']

        #tmdb api only allows up to 500 pages maximum
        if pages > 501:
            total_pages[year] = 500
        elif pages == 0:
            pass
        else:
            total_pages[year] = pages

        #log update
        print(f"Year {year} with {pages} pages")

        #delay next request
        time.sleep(delay)

    print("DONE: Collected total number of pages per year")

    #dictionary to contain year and tmdb ids
    tmdb_ids = []

    # (2) collect top most popular tmdb ids per year
    print("START: Collecting top most popular tmdb movies per year...")

    for year, pages in total_pages.items():
        for page in range(1, pages+1):
            movies = response_json(f'{url}&primary_release_year={year}&page={page}')
            ids = [movie['id'] for movie in movies['results']]
            tmdb_ids.extend(ids)

            #delay next request
            time.sleep(delay)
    
        #log update
        print(f"Scraped id for year {year}")

    print("DONE: Collected top most popular tmdb movies per year")

    #list containing dataframes
    df_list = []

    # (3) collect imdb ids plus the cast and crew info of the movie
    print("START: Collecting cast and crew info for each movie...")

    for movie_id in tmdb_ids:
        #dictionaries for data structure
        df_structure = {
            'tmdb_id':movie_id,
            'imdb_id':'',
            'tmdb_person_id':[],
            'name':[],
            'gender':[],
            'department':[],
            'job':[],
            'credit_type':[]
        }

        url = f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}'
        movies = response_json(f'{url}&append_to_response=credits')

        #get imdb id of movie for merging with imdb datasets
        df_structure['imdb_id'] = movies['imdb_id']

        #get cast and crew info
        casts = movies['credits']['cast']
        crews = movies['credits']['crew']

        for cast in casts:
            df_structure['gender'].append(cast['gender'])
            df_structure['tmdb_person_id'].append(cast['id'])
            df_structure['department'].append(cast['known_for_department'])
            df_structure['name'].append(cast['name'])
            df_structure['job'].append('Actor')
            df_structure['credit_type'].append('cast')

        for crew in crews:
            df_structure['gender'].append(crew['gender'])
            df_structure['tmdb_person_id'].append(crew['id'])
            df_structure['department'].append(crew['known_for_department'])
            df_structure['name'].append(crew['name'])
            df_structure['job'].append(crew['job'])
            df_structure['credit_type'].append('crew')

        #convert to dataframe and append to list
        df_list.append(pd.DataFrame(df_structure))

        #log update
        print(f"Scraped cast and crew info for {movie_id}")

        #delay next request
        time.sleep(delay)

    df_tmdb = pd.concat(df_list).reset_index(drop=True)

    print("DONE: Collected cast and crew info for each movie")

    #get tmdb person ids
    ids = df_tmdb['tmdb_people_id'].to_list()

    #empty list to store imdb person ids
    imdb_people = []

    # (4) get imdb id of each crew and cast of each movie
    print("START: Getting imdb person id for each crew and cast...")

    for id in ids:
        url = f'https://api.themoviedb.org/3/person/{id}/external_ids?api_key={API_KEY}'
        person = response_json(f'{url}')

        #add to list
        imdb_people.append(person['imdb_id'])

        #log update
        print(f"Extracted imdb person id for tmdb {id}")

        #delay next request
        time.sleep(delay)

    #update final dataframe
    df_tmdb['imdb_person_id'] = imdb_people

    print("DONE: Dataframe created and updated")

    return df_tmdb


if __name__=='__main__':

    config = ConfigParser()
    config.read('/home/jdtganding/Documents/bechdel-movies-project/api_keys.cfg')

    API_KEY = config.get('tmdb', 'api_key')
    df = get_tmdb_data(API_KEY=API_KEY, from_year=2023)

    #save final dataframe as a csv file
    path = '/home/jdtganding/Documents/bechdel-movies-project/data'
    df.to_csv(f'{path}/TMDB2023Results.csv', index=False)

    #show sample
    df.sample(10)