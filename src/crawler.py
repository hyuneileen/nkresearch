import requests
from bs4 import BeautifulSoup
import json
import collections
import math
import logging
from custom_logger import progressbar
import time

logger = logging.getLogger("Data Generator")
logger.setLevel(logging.DEBUG)

def site_crawler():
    """
    Script that recursively crawls the http://www.ryongnamsan.edu.kp website
    to fetch all journal listing information. STAGE (1) of the creation process. 

    How to use 
    ----------------------
    (1) Go to your computer's command line (AKA terminal, prompt, shell)
    (2) Change directories (cd) into this repository by entering
    >>> cd nkresearch 
    or whatever the folder name this repository is saved as
    (3) Run the crawler by hitting
    >>> python src/crawler.py 


    Output
    ----------------------
    (1) src/crawled_info.json
    └─ stores raw information crawled from the site
    ── list of list in form [[title,author,url]]
    (2) src/journal_info.json
    └─ holds journal-specific information for both recursion & metadata later
    ── dictionary in form {journal idx:[journal subject name, language, years, 
                            int(current count), int(actual count)]}


    Notes
    ----------------------
    To minimize server harm, site_crawler() has a "shallow crawl" and a "deep crawl"
    section. site_crawler() first heads to the front page, where it extracts the 
    indices of the different journals, i.e. different subjects. All article listing info 
    on the front page is parsed into [title,author,url] and stored in crawled_info. 
    
    Not all listings are linked to the front page (for some reason), so recursion is prepped
    by fetching the "listing count" for the indicies collected of the different journals. 
    Each index is checked whether all listings have been collected by comparing the current
    list vs. the supposed "listing count" on the site. 

    A "deep crawl" for that index is initiated iff the count is less. The crawler visits the
    journal index to extract the years of publication; each year is visited to extract the
    journal numbers published during that year (year=volume); the above [title,author,url] 
    process is repeated for each number in the year in the index,
   
    e.g.,

        NATURAL SCIENCE
                    └─ 2019
                          └─ no 1   <- extract from here
                          └─ no 2
                    └─ 2015
                          └─ no 1
                          └─ no 2
                    └─ 2014
                          └─ no 1
                          └─ no 2
                          └─ no 3
    
    """

    ## journal_info = {journal idx:[journal subject name, language, years, 
    #                   int(current count), int(actual count)]}
    journal_info = {}

    ## crawled_info = [[title,author,url]]
    ## actually collected data; 
    crawled_info = []
    
    ## split workload by lang; hold in temporary "temp"
    ## combined later
    languages = ["en","ko"]
    logger_lang = {"en":"English","ko":"Korean"}

    ## begin !
    for lang in languages:
        
        logger.info("Beginning crawler for %s journal...",logger_lang[lang])
        
        temp = []
        ## getting total count
        soup = BeautifulSoup(requests.get("http://www.ryongnamsan.edu.kp/univ/"+lang+"/research/journals").content, "html.parser")
        total_listings = soup.find("input", {"id": "totalListCnt"})
        if not total_listings:
            continue
        total_listings = total_listings.get('value')
        total_listings = int(total_listings.replace(" ",""))
        
        ## max_lim = max value for ?cp= calculated by 
        ## dividing total entries w/ amount (17) listed on each page
        max_lim = int(math.ceil(total_listings/17))  ## round up; aka ceiling
        
        ## get subjects (journal name); map to journal numbers
        ## used later as metadata
        for i,tag in enumerate(soup.find_all('nobr')):
            if i not in [0,len(soup.find_all('nobr'))-1]:
                subject = tag.text
                journal_idx = tag.find('a')['href']
                journal_idx = int(journal_idx.split('/')[-1].strip())
                journal_info[int(journal_idx)] = [subject]
        
        ## first page doesn't have ?cp=
        ## so append collection now
        ## it's already loaded too
        for d in soup.find_all("a", {"class": "journal-title"},href=True):
            title = d.text.strip()
            author = d.findNext("div", {"class": "list-author"}).text.strip()
            url = d['href']
            temp.append([title,author,url])

        ## beginning other pages...
        ## keep track of how many found
        logger.info("Parsing %s pages...",max_lim)
        for i in progressbar(list(range(1,max_lim+1)),"Parsing: ",40):
            soup = BeautifulSoup(requests.get("http://www.ryongnamsan.edu.kp/univ/"+lang+"/research/journals?cp="+str(i)).content, "html.parser")
            for d in soup.find_all("a", {"class": "journal-title"},href=True):
                title = d.text.strip()
                author = d.findNext("div", {"class": "list-author"}).text.strip()
                url = d['href']
                temp.append([title,author,url])
                time.sleep(2)
        
        ## remove duplicates
        temp = set(tuple(x) for x in temp)
        temp = [list(x) for x in temp]
        crawled_info.append(temp)

        ## get current collected listing counts per subject
        ## current_count = {int(journal_idx) : current count}
        current_count = collections.Counter(int(row[2].split("/")[5]) for row in temp)

        logger.info("Parsing indvidual pages for metadata...")
        ## parsing indvidual pages for metadata
        for journal_idx,count in current_count.items():
            journal_info[int(journal_idx)].append(lang)
            soup = BeautifulSoup(requests.get("http://www.ryongnamsan.edu.kp/univ/"+lang+"/research/journals/"+str(journal_idx)).content, "html.parser")
            
            ## append since its already loaded
            for d in soup.find_all("a", {"class": "journal-title"},href=True):
                title = d.text.strip()
                author = d.findNext("div", {"class": "list-author"}).text.strip()
                url = d['href']
                temp.append([title,author,url])

            ## getting actual listing count
            acutal_count = soup.find("input", {"id": "totalListCnt"})
            if not acutal_count:
                continue
            acutal_count = acutal_count.get('value')
            acutal_count = int(acutal_count.replace(" ",""))

            ## getting publication years...
            years = []
            for i,tag in enumerate(soup.find_all('nobr')):
                v = tag.find("a", {"class": "j-pubyear"})
                if v:
                    if (v.text.strip()).isdigit():
                        years.append(int(v.text.strip()))
            journal_info[int(journal_idx)].append(years)
            journal_info[int(journal_idx)].append(0)
            journal_info[int(journal_idx)].append(acutal_count)
        
        ## remove duplicates
        temp = set(tuple(x) for x in temp)
        temp = [list(x) for x in temp]
        crawled_info.append(temp)

        ## get current collected listing counts per subject
        ## current_count = {int(journal_idx) : current count}
        current_count = collections.Counter(int(row[2].split("/")[5]) for row in temp)
        
        ## update journal info with current count
        for journal_idx,count in current_count.items():
            journal_info[int(journal_idx)][3] = int(count)
    
    crawled_info = [item for sublist in crawled_info for item in sublist]

    ## check if crawled all...
    ## or deep crawl...
    
    logger.info("Checking for missing papers...")
    temp = []
    for journal_idx,v in journal_info.items():
        journal_idx = int(journal_idx)
        # check actual count w current count
        if v[3] == v[4]:
            logger.info("Found all %s papers for journal %s. Moving to next...",v[3],journal_idx)
            continue
        else:
            logger.debug("Current collection for journal %s is: %s. Need %s",journal_idx,v[3],v[4])
            logger.info("Re-crawling site for the missing %s papers...",v[4]-v[3])
            ## missing some articles
            lang = v[1]
            journal_url = "http://www.ryongnamsan.edu.kp/univ/"+lang+"/research/journals/"+str(journal_idx)
            soup = BeautifulSoup(requests.get(journal_url).content, "html.parser")
            ## getting publication years...
            years = []
            for tag in soup.find_all('nobr'):
                v = tag.find("a", {"class": "j-pubyear"})
                if v:
                    if (v.text.strip()).isdigit():
                        years.append(int(v.text.strip()))
            for year in years:
                time.sleep(2)
                soup = BeautifulSoup(requests.get(journal_url+"/"+str(year)).content, "html.parser")
                ## journal no
                no = []
                for tag in soup.find_all('nobr'):
                    w = tag.find("a", {"class": "j-num"})
                    if w:
                        w = int(w.get('data-id'))
                        if w != 0:
                            no.append(w)
                ## journal vol within issue
                for n in no:
                    time.sleep(2)
                    soup = BeautifulSoup(requests.get(journal_url+"/"+str(year)+"/"+str(n)).content, "html.parser")
                    for d in soup.find_all("a", {"class": "journal-title"},href=True):
                        title = d.text.strip()
                        author = d.findNext("div", {"class": "list-author"}).text.strip()
                        url = d['href']
                        temp.append([title,author,url])
                    
                    ## if short, stop at first page
                    total_listings = soup.find("input", {"id": "totalListCnt"})
                    if total_listings:
                        total_listings = total_listings.get('value')
                        total_listings = int(total_listings.replace(" ",""))
                        ## finding next pages
                        if total_listings < 12:
                            continue
                        else:
                            max_lim = int(math.ceil(total_listings/17))  ## round up; aka ceiling
                            logger.info("Re-fetching journal %s for year %s no %s",journal_idx,year,n)
                            logger.info("Parsing %s pages...",max_lim)
                            for i in progressbar(list(range(1,max_lim+1)),"Parsing: ",40):
                                time.sleep(2)
                                soup = BeautifulSoup(requests.get(journal_url+"/"+str(year)+"/"+str(n)+"?cp="+str(i)).content, "html.parser")
                                for d in soup.find_all("a", {"class": "journal-title"},href=True):
                                    title = d.text.strip()
                                    author = d.findNext("div", {"class": "list-author"}).text.strip()
                                    url = d['href']
                                    temp.append([title,author,url])
    ## remove duplicates
    crawled_info+=temp
    crawled_info = set(tuple(x) for x in crawled_info)
    crawled_info = [list(x) for x in crawled_info]

    ## update journal info with current count
    current_count = collections.Counter(int(row[2].split("/")[5]) for row in crawled_info)
    for journal_idx,count in current_count.items():
        journal_info[int(journal_idx)][3] = int(count)
    
    ## write to json
    with open('src/crawled_info.json', 'w') as f:
        json.dump(crawled_info, f, ensure_ascii=False, indent=1)
    with open('src/journal_info.json', 'w') as f:
        json.dump(journal_info, f, ensure_ascii=False, indent=1)

    return crawled_info,journal_info


if __name__ == "__main__":
    site_crawler()