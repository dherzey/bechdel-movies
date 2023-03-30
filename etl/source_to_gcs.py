# from pathlib import Path
# from prefect import task, flow
# from prefect_gcp.cloud_storage import GcsBucket

import sys

sys.path.append("../bechdel-movies-project/scraper")
from scrape_oscars_db import *
from scrape_movie_data import *


print('Done')