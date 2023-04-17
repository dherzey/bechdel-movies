# Bechdel movies and the Academy Awards
This project is part of the final requirement for DataTalks.Club's data engineerring bootcamp. 

## Problem Statement


## Data Architecture


## Collecting Data From Source
Data is collected from the following web sources and database:
- Academy Awards database
- BechdelTest.com API
- IMDB available datasets
- The Movie Database API (<i>to be added</i>)

### Scraping the Oscars database
Results for the Academy Awards nominees and winners from the first Academy Awards until the latest are collected from the online Academy Awards database (https://awardsdatabase.oscars.org/). In order to acquire the full HTML source of the award results, Selenium was used to interact with the site and collect its page source which was then parsed using BeautifulSoup for data extraction. See the full code in [scrape_oscars_db.py](https://github.com/dherzey/bechdel-movies-project/blob/main/scraper/scrape_oscars_db.py). Instructions can also be found in [scraper](https://github.com/dherzey/bechdel-movies-project/blob/main/scraper) on how to install Selenium and BeautifulSoup. 

### Collecting Bechdel test movie list
The Bechdel test movie list and their Bechdel scores are collected from http://bechdeltest.com/. We used the given API to return all the site's movie list using Python. The simple function to collect this data using `requests()` can be found in [source_to_gcs.py](https://github.com/dherzey/bechdel-movies-project/blob/main/etl/source_to_gcs.py) file.

**NOTE:** Based on the API documentation, the owner requests any user to query the `getAllMovies` method as little as possible due to a shared hosting plan.

### Collecting movie data from IMDB dataset
IMDB datasets are available to download from https://www.imdb.com/interfaces/. For this project, the following datasets will be used:
- title.basics.tsv.gz
- title.principals.tsv.gz
- title.crew.tsv.gz
- title.ratings.tsv.gz
- name.basics.tsv.gz

The data will be read in chunks and converted into a `.parquet` file for loading in GCS. See [source_to_gcs.py](https://github.com/dherzey/bechdel-movies-project/blob/main/etl/source_to_gcs.py) for the functions to collect and transform IMDB data.

### Using TMDB API to collect movie data (to be added)
We use the TMDB API to collect the top popular movies of each year along with their cast and crew info. See the full code under `get_tmdb_data()`in [scrape_movie_data.py](https://github.com/dherzey/bechdel-movies-project/blob/main/scraper/scrape_movie_data.py).

<br>
<i>This product uses the TMDB API but is not endorsed or certified by TMDB.</i>
<img src="https://www.themoviedb.org/assets/2/v4/logos/v2/blue_long_2-9665a76b1ae401a510ec1e0ca40ddcb3b0cfe45f1d51b77a308fea0845885648.svg" alt="TMDB logo" style="height: 50px; width:150px"/>

## Configure cloud resources using Terraform
Resources are configured and provisioned using Terraform. This would need GCP service account credentials in order to create a Google Cloud Storage bucket and a BigQuery dataset in the indicated GCP project. See [Terraform folder](https://github.com/dherzey/bechdel-movies-project/blob/main/terraform) for more info.

## Setting up workflow orchestrator and deployments
The Python files under the [etl folder](https://github.com/dherzey/bechdel-movies-project/blob/main/etl) contains the scripts for the whole workflow. Using Prefect blocks and flows, we can create Prefect deployments that will run the workflows for the extraction and loading of data to GCS and BigQuery. See more info in the [etl folder](https://github.com/dherzey/bechdel-movies-project/blob/main/etl).

## Loading data from source to GCS

## Loading data from storage to BigQuery data warehouse