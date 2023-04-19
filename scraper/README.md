## Installing Selenium to interact with the Oscars database
Since the Oscars scraper uses the Firefox browser for its Selenium driver, make sure that Firefox is initially installed in the host machine.

```bash
# pip install the selenium package (if not yet installed)
pip install selenium

# download the Firefox driver from https://github.com/mozilla/geckodriver/releases
wget https://github.com/mozilla/geckodriver/releases/download/v0.32.2/geckodriver-v0.32.2-linux32.tar.gz

# decompress the file
tar -xvzf geckodriver*

# move the executable to /usr/loca/bin/
sudo mv geckodriver /usr/local/bin/
```