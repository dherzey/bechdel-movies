# Bechdel movies and the Academy Awards
This project is part of the final requirement for DataTalks.Club's data engineerring bootcamp. 

## Problem Statement

## Data Architecture


## Collecting Data From Source
Data is collected from the following web sources and database:
- Academy Awards database
- Bechdel Test movie list (via API)
- IMDB collected datasets
- The Movie Database (via API)

### Scraping the Oscars database
Results for the Academy Awards nominees and winners from the first Academy Awards until the latest as collected from the online Academy Awards database (https://awardsdatabase.oscars.org/). In order to acquire the full HTML source of the award results, Selenium was used to interact with the site and collect its page source which was then parsed using BeautifulSoup for data extraction. See the full code in [scrape_oscars_db.py](https://github.com/dherzey/bechdel-movies-project/blob/main/scraper/scrape_oscars_db.py).

### Scraping Bechdel test movie list
The Bechdel test movie list and their Bechdel scores are collected from http://bechdeltest.com/. We used the given API to return all the site's movie list using Python.

**NOTE:** Based on the API documentation, the owner requests any user to query the `getAllMovies` method as little as possible due to a shared hosting plan.

### Collecting movie data from IMDB dataset
IMDB datasets are available to download from https://www.imdb.com/interfaces/. For this project, the following datasets will be used:
- title.basics.tsv.gz
- title.principals.tsv.gz
- title.ratings.tsv.gz

### Using TMDB API to collect movie data
We use the TMDB API to collect the top popular movies of each year along with their cast and crew info. See the full code under `get_tmdb_data()`in [scrape_movie_data.py](https://github.com/dherzey/bechdel-movies-project/blob/main/scraper/scrape_movie_data.py).

<img src="https://www.themoviedb.org/assets/2/v4/logos/v2/blue_long_2-9665a76b1ae401a510ec1e0ca40ddcb3b0cfe45f1d51b77a308fea0845885648.svg" alt="TMDB logo" style="height: 50px; width:150px"/>

***This product uses the TMDB API but is not endorsed or certified by TMDB.***

## Configure cloud resources using Terraform

## Setting up workflow orchestrator and deployments

## Loading data from source to GCS

## Loading data from storage to BigQuery data warehouse