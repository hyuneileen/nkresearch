
from bs4 import BeautifulSoup
import requests
import subprocess
import json
from multiprocessing.pool import ThreadPool 
from urllib.parse import urlparse
import json
from concurrent.futures import ThreadPoolExecutor
import sys
import traceback
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry 
import argparse

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



def geolocation(geo_worker,geo_iter):
    with open('gs/dns-lookup.json',"r") as f:
        dns = json.load(f)
    dns_data = []
    for citation in dns:
        if "href" not in citation[0]:
            continue
        if "ip-address" not in citation[-1]:
            continue
        key_list = ["MD5","Citation","href"]
        dict1 = {}
        for key in key_list:
            dict1[key] = citation[0][key]
        dns_dict = {**dict1,**citation[-1]}
        dns_data.append(dns_dict)

    unique_ip = [dns["ip-address"] for dns in dns_data]
    unique_ip = list(set(unique_ip))

    with ThreadPoolExecutorStackTraced(max_workers=geo_worker) as executor:
        futures = [executor.submit(geolocation_lookup, ip_address) for ip_address in unique_ip]
        geolocation = []
        connection_lost = []
        for i,future in enumerate(futures):
            try:
                geolocation.append([unique_ip[i],future.result()])
            except:
                connection_lost.append(unique_ip[i])

    reruns  = connection_lost
    for iter in list(range(geo_iter)):
        with ThreadPoolExecutorStackTraced(max_workers=geo_worker) as executor:
            if not reruns:
                geolocation+=rerun_geolocation
                with open("gs/geolocation.json", 'w') as f:
                    json.dump(geolocation, f, ensure_ascii=False, indent=2)
                    return 
            futures = [executor.submit(geolocation_lookup, ip_address) for ip_address in reruns]
            rerun_geolocation = []
            reruns_todo = []
            for i,future in enumerate(futures):
                try:
                    rerun_geolocation.append([reruns[i],future.result()])
                except:
                    reruns_todo.append(reruns[i])
            reruns = reruns_todo
                
    geolocation+=rerun_geolocation
    with open("gs/geolocation.json", 'w') as f:
        json.dump(geolocation, f, ensure_ascii=False, indent=2)
    return 

def geolocation_lookup(ip_address):
    # returns dict
    url = "http://ip-api.com/json/"+str(ip_address)+"?fields=28897279"
    payload = {'api_key': '519867c2ad1206af28e018b1806db456', 'url':url, 'render': 'false'}
    soup = BeautifulSoup(requests.get('http://api.scraperapi.com',params=payload).content, "html.parser")
    return json.loads(str(soup))

