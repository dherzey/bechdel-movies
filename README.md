# Bechdel movies and the Academy Awards
This project is part of the final requirement for DataTalks.Club's data engineerring bootcamp. 

## Problem Statement

## Data Architecture

## Requirements

## Collecting Data From Source
Data is collected from the following web sources and database:
- Academy Awards database
- Bechdel Test movie list
- imdb datasets

### Scraping the Oscars database
Results for the Academy Awards nominees and winners from the first Academy Awards until the latest as collected from the online Academy Awards database (https://awardsdatabase.oscars.org/). In order to acquire the full HTML source of the award results, Selenium was used to interact with the site and collect its page source which was then parsed using BeautifulSoup for data extraction. See the full code in [scrape_oscars_db.py](https://github.com/dherzey/bechdel-movies-project/blob/main/codes/scrape_oscars_db.py).

### Scraping Bechdel test movie list
```python
import io
import requests
import pandas as pd

html = requests.get('http://bechdeltest.com/api/v1/getAllMovies').content
df = pd.read_json(io.StringIO(html.decode('utf-8')))
```

### Collecting movie data from IMDB dataset

### Using tmDB API to collect movie data


## Configure cloud resources using Terraform

## Setting up workflow orchestrator and deployments

## Loading data from source to GCS

## Loading data from storage to BigQuery data warehouse