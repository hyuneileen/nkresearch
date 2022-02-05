import json
import requests
import urllib3
import os
from crawler import site_crawler
from custom_logger import CustomFormatter, progressbar
import logging
from metadata_generator import metadata_main
import argparse
import time

logger = logging.getLogger("Data Generator")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


def main(file):
    """
    The main function that generates all files in this repostory in a three process:

    Stage_1: recursively crawls the site by calling site_crawler() in crawler.py
    Stage_2: generates & hierarchically calls article download links in bank
    Stage_3: synthesizes metadata by calling metadata_main() in metadata_generator.py


    How to use 
    ----------------------
    To replicate the full experiment, 
    (1) Go to your computer's command line (AKA terminal, prompt, shell)
    (2) Change directories (cd) into this repository by entering
    >>> cd nkresearch 
        or whatever the folder name this repository is saved as
    (3) Run the main script by entering
    >>> python src/data_generator.py
    (4) Sleep. ETA is 10 hours. Do not adjust the delays. THE SITE WILL CRASH.
        Less important, to not anger the rocket man
    
    Stage_1 and Stage_3 are external functions, thus can be called separately.
    For example, to only recreate the metadata generation process, run
    >>> python src/metadata_generator.py
    See individual code files for more detailed documentation.


    Output
    ----------------------
    In order, each stage of main() generates: 
    Stage_1) crawled_info.json, journal_info.json;
    Stage_2) all 6500+ article PDFs under /journals; 
    Stage_3) csv files under /streams, txt files under /corpus, 
        completed bank.json, csv,excel,pkl of bank & multiindex under/metadata, 
        7 Excel sheets under /excel.


    Notes
    ----------------------
    main() serves as the intermediary of (1) and (3) by deducing download links and 
    iteractively requesting them. In (2), raw information from the (1) crawl is parsed 
    into 6 functional forms (hash, directory, title, author, host link, download link), 
    which initiates the bank. The download links are subseqeuntly called and 
    stored in the appropriate directories. 

    """
    # stage 1: 
    # initiate crawler
    logger.info("Stage 1: Crawling site...")
    site_crawler()
    
    # now processing collection....
    
    ######################################################################
    logger.info("Done crawling. Now processing raw data...")


    # stage 2:
    # downloading the data
    ######################################################################

    logger.info("Stage 2: Now downloading data...")
    # reopen json for safety
    with open('bank.json', 'r') as f:
        bank = json.load(f)
    with open('src/directory_mapping.json', 'r') as f:
        directory_mapping = json.load(f)
    
    ## now fetching data...
    session = requests.Session()
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    for k,v in progressbar(bank.items(), "Downloading: ", 40):
        download_link = v["Download Link"]
        md5hash = k
        directory = directory_mapping[md5hash]
        r = requests.get(download_link)
        if not os.path.exists(directory): ## making directory
            os.makedirs(directory)
        with open(directory+md5hash+'.pdf', 'wb') as f:
            f.write(r.content)
        time.sleep(2) 
    
    logger.info("Finished downloading all papers.")

    # stage 3:
    # metadata synthesis
    ######################################################################
    
    logger.info("Stage 3: Now generating metadata in bank...")
    metadata_main(file)
    logger.info("Done generating metadata. Operation is complete !")
    logger.info("Terminating...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'data generator')
    parser.add_argument('-f','--file', type=str, default='pdf')
    args = parser.parse_args()
    main(args.file)