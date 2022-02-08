import json
import requests
from bs4 import BeautifulSoup
import argparse
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor


class ThreadPoolExecutorStackTraced(ThreadPoolExecutor):
    def submit(self, fn, *args, **kwargs):
        """Submits the wrapped function instead of `fn`"""

        return super(ThreadPoolExecutorStackTraced, self).submit(
            self._function_wrapper, fn, *args, **kwargs)

    def _function_wrapper(self, fn, *args, **kwargs):
        """Wraps `fn` in order to preserve the traceback of any kind of
        raised exception
        """
        try:
            return fn(*args, **kwargs)
        except Exception:
            raise sys.exc_info()[0](traceback.format_exc())  # Creates an
                                                             # exception of the
                                                             # same type with the
                                                             # traceback as
                                                             # message


def gs(idx,references,api_key):
    [md5hash,citation] = references[idx]
    if "종합대학학보" in citation: # north korean works arent indexed on gs
        return {"MD5":md5hash,"Citation":citation} 
    url = 'https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q='+str(citation)+'&btnG='
    payload = {'api_key': api_key, 'url':url, 'render': 'false'}
    soup = BeautifulSoup(requests.get('http://api.scraperapi.com',params=payload).content, "html.parser")
    
    for listing in soup.find_all("div", {"id": "gs_res_ccl_mid"}):
        bold = []
        article = []
        
        if (len(listing.find_all("div", {"class": "gs_r gs_or gs_scl"}))) <=4:
            for i,tt in enumerate(listing.find_all("div", {"class": "gs_r gs_or gs_scl"})):
            # getting article info
                temp = {}
                temp["MD5"] = md5hash
                temp["Citation"] = citation
                for t in tt.find_all("h3", {"class": "gs_rt"}):
                    if not t.find_all('a', href=True) and t.find_all('span'):
                        for span in t.find_all('span'):
                            if span.get('id'):
                                temp["Title"] = span.text
                                temp["id"] = span.get('id')
                    elif t.find_all('a', href=True):
                        for a in t.find_all('a', href=True):
                            temp["Title"] = a.text
                            temp["href"] = a['href']
                            temp["id"] = a['id']
                article.append(temp)
            if article:
                match = article[0]
                if "id" not in match:
                    return match
                gs_id = match["id"].strip()
                cite_url = "https://scholar.google.com/scholar?q=info:"+gs_id+":scholar.google.com/&output=cite&scirp=0&hl=en"
                payload = {'api_key': api_key, 'url':cite_url, 'render': 'false'}
                soup = BeautifulSoup(requests.get('http://api.scraperapi.com',params=payload).content, "html.parser")
                mla = soup.find("div", {"class": "gs_citr"})
                if mla:
                    mla = mla.text
                    match["MLA"] = mla
                    return match
            
        for i,tt in enumerate(listing.find_all("div", {"class": "gs_r gs_or gs_scl"})):
            # getting article info
            temp = {}
            temp["MD5"] = md5hash
            temp["Citation"] = citation
            for t in tt.find_all("h3", {"class": "gs_rt"}):
                for a in t.find_all('a', href=True):
                    temp["Title"] = a.text
                    temp["href"] = a['href']
                    temp["id"] = a['id']
            for ii,journal_info in enumerate(tt.find_all("div", {"class": "gs_a"})):
                bold.append(list(set([b.string for b in journal_info.findAll('b')])))
            article.append(temp)
        if not bold:
            return {"MD5":md5hash,"Citation":citation}
        if bold.index(max(bold, key=len)) < 3:
            match = article[bold.index(max(bold, key=len))]
            if "id" not in match:
                return match
            gs_id = match["id"].strip()
            cite_url = "https://scholar.google.com/scholar?q=info:"+gs_id+":scholar.google.com/&output=cite&scirp=0&hl=en"
            payload = {'api_key': api_key, 'url':cite_url, 'render': 'false'}
            soup = BeautifulSoup(requests.get('http://api.scraperapi.com',params=payload).content, "html.parser")
            mla = soup.find("div", {"class": "gs_citr"})
            if mla:
                mla = mla.text
                match["MLA"] = mla
                return match
            else:
                return match
    return {"MD5":md5hash,"Citation":citation}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'gs query')
    parser.add_argument('-s','--start', type=int)
    parser.add_argument('-e','--end', type=int)
    parser.add_argument('-r','--run', type=str)
    parser.add_argument('-k','--key', help='<Required> Set flag', required=True)
    args = parser.parse_args()
    index = list(range(args.start,args.end))

    if args.run == "main":
        with open('bank.json',"r") as f:
            bank = json.load(f)
        references = []
        for md5hash,metadata in bank.items():
            if metadata["References"]:
                for citation in metadata["References"]:
                    if citation:
                        references.append([md5hash,str(citation)])

    if args.run == "rerun":
        with open("refs/connection_lost","r") as f:
            references = json.load(f)

    with ThreadPoolExecutorStackTraced(max_workers=10) as executor:
        index = list(range(args.start,args.end))
        print("Now starting index",str(args.start))
        futures = [executor.submit(gs, idx, references, args.key) for idx in index]
        fetched = []
        connection_lost = []
        type_error = []
        for i,future in enumerate(futures):
            try:
                fetched.append(future.result())
            except requests.ConnectionError:
                connection_lost.append(references[index[i]])
            except TypeError:
                type_error.append(references[index[i]])
        fetched_fname = 'refs/fetched-'+str(args.start)+'-'+str(args.end)+'.json'
        connection_lost_fname = 'refs/connection_lost-'+str(args.start)+'-'+str(args.end)+'.json'
        type_error_fname = 'refs/type_error-'+str(args.start)+'-'+str(args.end)+'.json'
        with open(fetched_fname, 'w') as f:
            json.dump(fetched, f, ensure_ascii=False, indent=2)
        with open(connection_lost_fname, 'w') as f:
            json.dump(connection_lost, f, ensure_ascii=False, indent=2)
        with open(type_error_fname, 'w') as f:
            json.dump(type_error, f, ensure_ascii=False, indent=2)
        print("Finished up to index",str(args.end))
