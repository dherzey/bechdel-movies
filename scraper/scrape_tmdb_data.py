"""----------------------------------------------------------------------
Script for collecting movie data from the TMDB API. This can be added to
expand the Bechdel test porject in the future to account for the gender 
of the movie's cast and crew.

Last modified: April 2023
----------------------------------------------------------------------"""

import time
import requests
import pandas as pd
from datetime import date
from configparser import ConfigParser


def response_json(API_URL):
    """
    Uses Python requests to get content of specified url

    Arguments:
        API_URL: url of API

    Returns:
        json object of the response. 
        If status code error, returns "REQUEST ERROR" string
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
        - from_year: beginning year to start collecting movie data.
                     Default set to 1874. 
        - to_year: final year to end collecting movie data.
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

    #delay next part by 1 minute
    time.sleep(60)

    #list containing dataframes
    df_list = []

    # (3) collect imdb ids plus the cast and crew info of the movie
    print("START: Collecting cast and crew info for each movie...")

    for index, movie_id in enumerate(tmdb_ids):
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

        if (index % 50 == 0) and (index != 0):
            print("Delaying for another 25 seconds...")
            time.sleep(25)

    df_tmdb = pd.concat(df_list).reset_index(drop=True)

    print("DONE: Collected cast and crew info for each movie")

    #delay next part by 1 minute
    time.sleep(60)

    #get tmdb person ids
    ids = df_tmdb['tmdb_person_id'].to_list()

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

    print("DONE: Dataframe created and updated\n")

    return df_tmdb


if __name__=='__main__':

    config = ConfigParser()
    config.read('/home/jdtganding/Documents/bechdel-movies-project/api_keys.cfg')
    API_KEY = config.get('tmdb', 'api_key')

    path = '/home/jdtganding/Documents/bechdel-movies-project/data/tmdb'

    for year in range(1900, 1911):

        try:
            df = get_tmdb_data(API_KEY=API_KEY, from_year=year, to_year=year)

            #save final dataframe as a csv file
            df.to_csv(f'{path}/TMDBResults_{year}.csv', index=False)

        except ValueError:
            print(f"No data for year {year}\n")
            pass

        #delay next request by 1 min
        time.sleep(60)