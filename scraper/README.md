## Extracting data
This folder contains the Python scripts used to web scrape or collect data from different web sources.

### I. Installing Selenium and BeautifulSoup to scrape the Oscars database

```bash
#pip install the selenium package
pip install selenium

#download the Firefox driver from https://github.com/mozilla/geckodriver/releases
wget https://github.com/mozilla/geckodriver/releases/download/v0.32.2/geckodriver-v0.32.2-linux32.tar.gz

#decompress the file
tar -xvzf geckodriver*

#move the executable to /usr/loca/bin/
sudo mv geckodriver /usr/local/bin/

#we also install beautifulsoup for extracting data in our html files
pip install beautifulsoup4
```

### II. Creating TMDB account to generate API key