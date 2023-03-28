"""----------------------------------------------------------------------
Script for web scraping data in the Oscar Academy Awards database
Last modified: March 2023
----------------------------------------------------------------------"""

import re
import time
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException


def scrape_oscars_data(delay=60):
    """
    Interacts with the Academy Award database using Selenium to return 
    award results from the first until the latest awarding year ceremony.

    Arguments:
        delay: time to wait for results page to load in seconds.
               default set at 60 seconds

    Returns:
        HTML page source of the site
    """

    options = webdriver.FirefoxOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')

    driver = webdriver.Firefox(options=options)
    driver.get("https://awardsdatabase.oscars.org/") 

    #select award categories
    xpath = "//button[contains(@class,'awards-basicsrch-awardcategory')]"
    driver.find_element(By.XPATH, xpath).click()

    xpath = "//b[contains(text(),'Current Categories')]"
    driver.find_element(By.XPATH, xpath).click()

    #select starting award year
    xpath = "//button[contains(@class,'awards-advsrch-yearsfrom')]"
    driver.find_element(By.XPATH, xpath).click()

    xpath = "//div[@class='btn-group multiselect-btn-group open']//input[@value='1']"
    driver.find_element(By.XPATH, xpath).click()

    #select ending award year
    xpath = "//button[contains(@class,'awards-advsrch-yearsto')]"
    driver.find_element(By.XPATH, xpath).click()

    xpath = "//div[@class='btn-group multiselect-btn-group open']//li"
    latest = len(driver.find_elements(By.XPATH, xpath))-2

    xpath = f"//div[@class='btn-group multiselect-btn-group open']//input[@value='{latest}']"
    driver.find_element(By.XPATH, xpath).click()

    #search to view results
    driver.find_element(By.XPATH, '//*[@id="btnbasicsearch"]').click()

    #wait for all results to show
    time.sleep(delay)

    try:
        #resultscontainer will contain all our needed Oscars data
        driver.find_element(By.XPATH, '//*[@id="resultscontainer"]')

    except NoSuchElementException as error:
        print(error)
        print(f"Needed element still not found after {delay} seconds delay")

    #get html source for BeautifulSoup extraction
    page_source = driver.page_source

    #close driver
    driver.close()
    print("Driver closed")

    return page_source


def extract_oscar_results(page_source):
    """
    Function which parses the page_source using BeautifulSoup 
    and turns it into the structured format of a dataframe.

    Arguments:
        page_source: HTML source of the site

    Returns:
        Dataframe with the following columns:
            - AwardYear: the year the award was received
            - AwardCeremonyNum: the nth annual ceremony award
            - Movie: the title of the nominated film
            - AwardCategory: the category the film was nominated for
            - AwardStatus: whether the film was only nominated or had won
    """

    soup = BeautifulSoup(page_source, "lxml")
    results_container = soup.find('div', {'id':'resultscontainer'})

    class_ = 'awards-result-chron result-group group-awardcategory-chron'
    award_year_all = results_container.find_all('div', class_=class_)

    #list to contain all dataframe for each award year
    oscars_results = []
    
    for award_year_group in award_year_all:
        #main structure of the dataframe
        df_structure = {
                        "AwardYear":'',
                        "AwardCeremonyNum":'',
                        "Movie":[],
                        "AwardCategory":[],
                        "AwardStatus": []
                    }

        #find the award year title
        award_year = award_year_group.find('div',class_='result-group-title')\
                                     .get_text(strip=True)

        #separate award year title to extract year
        key_split = award_year.split(" ")
        df_structure['AwardYear'] = key_split[0]
        df_structure['AwardCeremonyNum'] = re.findall(r'\d+',key_split[1])[0]
        
        #award category result subgroup (each contains award title and nominees)
        class_ = 'result-subgroup subgroup-awardcategory-chron'
        award_category_all = award_year_group.find_all('div',class_=class_)

        for award_category_group in award_category_all:
            #find award title
            award_title = award_category_group.find('div',class_='result-subgroup-title')\
                                              .get_text(strip=True)
            
            try:
                #find nominated movies
                movies = [movie.get_text(strip=True) for movie in award_category_group\
                               .find_all('div', class_='awards-result-film-title')]

                #remove duplicates
                movies = list(set(movies)) 

                #find winning movie/s
                winner_group = award_category_group.find('span', {'title':'Winner'})\
                                                   .find_next_sibling('div')

                winners = [movie.get_text(strip=True) for movie in winner_group\
                                .find_all('div', class_='awards-result-film-title')]

                #update df_structure movie and category lists
                if len(movies) > 0:
                    df_structure['Movie'].extend(movies)
                    repeat = list(np.repeat([award_title],len(movies)))
                    df_structure['AwardCategory'].extend(repeat)

                    #add winner/s
                    repeat = list(np.repeat(['nominated'],len(movies)))
                    for winner in winners:
                        repeat[movies.index(winner)] = 'won'

                    df_structure['AwardStatus'].extend(repeat)
        
            except AttributeError:
                pass

        #append dataframe to list
        oscars_results.append(pd.DataFrame(df_structure))

    #concatenate all award year into a single dataframe     
    return pd.concat(oscars_results).reset_index(drop=True)


if __name__=="__main__":

    page_source = scrape_oscars_data()
    print("DONE: Scraped Oscars data")

    results_df = extract_oscar_results(page_source)
    print("DONE: Extracted needed elements from HTML")
    print("DONE: Data formatted as a structured dataframe\n")

    print("Test sample results:")
    print(results_df.sample(10))