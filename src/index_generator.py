import json
import subprocess
import os
import argparse
import re


def query_caller(interval,iteration,api):
    # load bank
    with open('bank.json',"r") as f:
        bank = json.load(f)
    references = []
    for md5hash,metadata in bank.items():
        if metadata["References"]:
            for reference in metadata["References"]:
                if reference:
                    references.append([md5hash,str(reference)])
                else:
                    pass
        else:
            continue
    # calling main
    main_tups = tup_generator(interval,len(references))
    main_api_tups = api_distributor(main_tups,api)
    main_subprocess = subprocess_syntax(main_api_tups,"main")
    for command in main_subprocess: ### RUN here
        subprocess.call(command)
    # when main is done
    # doing connection issues
    post_run(api,main_tups,interval)

    for i in list(range(iteration)):
        if ftype_count("connection_lost") != 0:
            rerun_tups = tup_generator(interval,ftype_count("connection_lost"))
            ce,ce_fnames = compile_files("connection_lost",rerun_tups)
            te,te_fnames = compile_files("type_error",rerun_tups)
            remove_files(te_fnames)
            if not ce: # no more found
                fe,fe_fnames = compile_files("fetched",rerun_tups)
                with open("gs/fetched.json", 'w') as f:
                    json.dump(fe, f, ensure_ascii=False, indent=2)
                remove_files(ce_fnames)
                remove_files(fe_fnames)
                if os.path.isfile("gs/connection_lost.json"):
                    os.remove("gs/connection_lost.json")
                return 
            else:
                post_run(rerun_tups,interval)
        else:
            return

def post_run(api,current_tups,interval):
    fe,fe_fnames = compile_files("fetched",current_tups)
    te,te_fnames = compile_files("type_error",current_tups)
    ce,ce_fnames = compile_files("connection_lost",current_tups)
    with open("gs/fetched.json", 'w') as f:
        json.dump(fe, f, ensure_ascii=False, indent=2)
    remove_files(fe_fnames)
    remove_files(te_fnames)
    if not ce:
        remove_files(ce_fnames)
        return
    if ce:
        with open("gs/connection_lost.json", 'w') as f:
            json.dump(ce, f, ensure_ascii=False, indent=2)
        remove_files(ce_fnames)
        # rerunning connection errors
        rerun_tups = tup_generator(interval,len(ce))
        rerun_api_tups = api_distributor(rerun_tups,api)
        rerun_subprocess = subprocess_syntax(rerun_api_tups,"rerun")
        for command in rerun_subprocess: ### RUN here
            subprocess.call(command)
        return
    else:
        return

# chunking intervals
def tup_generator(interval,total):
    tups = [(i*interval,i*interval+interval) for i in list(range(total//interval))]+[(total-(total%interval),total)]
    return tups


def api_distributor(tups,api):
    api_divisor = len(tups)//len(api)+1
    api_sections = [tups[i:i + api_divisor] for i in range(0, len(tups), api_divisor)]
    if len(api_sections) != len(api):
        raise ValueError('API error')
    else:
        return (list(zip(api,api_sections)))

# subprocess maker
def subprocess_syntax(api_tups,runtype):
    syntax = []
    for (api_key,(ranges)) in api_tups:
        for (start,end) in ranges:
            syntax.append(["python","src/query.py","--start",str(start),
                        "--end",str(end),"--run",runtype,"--key",str(api_key)])
    return syntax


def compile_files(ftype,tups):
    # ftype = type_error, connection_lost, fetched
    compiled_content = []
    compiled_fnames = []
    for (start,end) in tups:
        fname = 'gs/'+ftype+'-'+str(start)+'-'+str(end)+'.json'
        try:
            with open(fname,"r") as f:
                comp = json.load(f)
                compiled_content+=comp
                compiled_fnames.append(fname)
        except FileNotFoundError:
            pass
    if ftype in compiled_fnames:
        compiled_fnames.remove(ftype)
    return compiled_content, compiled_fnames


def ftype_count(ftype):
    # ftype = type_error, connection_lost, fetched
    files = []
    for file in os.listdir("gs"):
        if file.startswith(ftype+"-"):
            files.append(file)
    if not files:
        return 0
    numbs = []
    for f in files:
        numbs.append(int(f.rsplit("-",1)[-1].replace(".json","")))
    return max(numbs)

def remove_files(fnames):
    for fname in fnames:
        if os.path.isfile(fname):
            os.remove(fname)
        else:
            pass


##------------------------------------------------------------------------------------
##------------------------------------------------------------------------------------

import re
def mla_parser(gs_data):
    mla_parsed = []
    exceptions = []
    for t in gs_data:
        temp = {}
        if "MLA" not in t:
            continue
        mla = t["MLA"]
        if '. "' in mla:
            author,rest = mla.split('. "',1)
            temp["author"] = author
            if '." ' in rest:
                title,rest2 = rest.split('." ',1)
                temp["title"] = title
                # journal proabably
                if ")" in rest2 and "Conference" not in mla:
                    year_cand = re.findall(r"\([0-9]{4}\)", rest2)
                    if len(year_cand) == 1:
                        temp["type"] = "article"
                        journal,rest3 = rest2.split(year_cand[0])
                        temp["year"] = re.findall(r"\((\d+)\)", year_cand[0])[0]
                        if journal:
                            number = re.findall("([0-9]+[.]+[0-9]+)", journal)
                            if not number:
                                volume = re.findall(r'\d+', journal)
                                temp["issue"] = ""
                                if volume: # volume no issue
                                    journal = journal.split(volume[0])
                                    temp["journal"] = journal
                                    temp["volume"] = volume[0]
                                    temp["start pg"],temp["end pg"] = page_numbers(rest3)
                                else:
                                    if "," not in journal:
                                        temp["journal"] = journal
                                    temp["volume"] = ""
                                    temp["start pg"],temp["end pg"] = page_numbers(rest3)
                            else:
                                temp["journal"] = journal.split(number[0])[0]
                                temp["volume"],temp["issue"] = number[0].split(".")
                                temp["start pg"],temp["end pg"] = page_numbers(rest3)
                            mla_parsed.append([t,temp])
                            continue
                        else:
                            temp["journal"] = ""
                            temp["volume"] = ""
                            temp["issue"] = ""
                            temp["start pg"],temp["end pg"] = page_numbers(rest3)
                            mla_parsed.append([t,temp])
                # conference papers
                elif "proceedings" in rest2.lower() or "conference" in rest2.lower():
                    if ". " not in rest2:
                        exceptions.append(t)
                        continue
                    conference,rest = rest2.split(". ",1)
                    temp["type"] = "proceedings"
                    temp["conference"] = conference
                    year = re.findall(r"\d{4}", rest)[0]
                    rest = re.sub(year,"",rest)
                    volume = re.findall(r'\d+', rest)
                    if volume:
                        temp["volume"] = volume[0]
                        rest = re.sub(volume[0],"",rest)
                        rest = re.sub("Vol.","",rest)
                        rest = re.sub('\W+',' ', rest)
                        temp["publisher"] = rest
                    else:
                        temp["volume"] = ""
                        rest = re.sub('\W+',' ', rest)
                        temp["publisher"] = rest
                    mla_parsed.append([t,temp])
            
                # journal pt2
                elif re.findall("([0-9]+[.]+[0-9]+)", rest2):
                    
                    number = re.findall("([0-9]+[.]+[0-9]+)", rest2)[0]
                    journal = rest2.split(number)[0].strip()
                    temp["journal"] = journal
                    temp["volume"], temp["issue"] = number.split(".")
                    rest = rest2.split(number)
                    if not rest[0].isdigit():
                        temp["publisher"] = rest[0].strip()
                        rest = rest[-1].replace(".","").strip()
                        if rest.isdigit():
                            temp["start pg"],temp["end pg"] = rest,rest
                            continue
            
                elif ". " in rest2: # (chapter in book) incollection
                    temp["type"] = "incollection"
                    booktitle, rest = rest2.split(". ", 1)
                    temp["booktitle"] = booktitle
                    if rest.strip() == ".":
                        temp["publisher"] = ""
                        temp["year"] = ""
                        continue
                    if re.findall(r"\d{4}", rest):
                        year = re.findall(r"\d{4}", rest)[-1]
                        publisher,pages = rest.rsplit(year,1)
                        temp["year"] = year
                        temp["start pg"],temp["end pg"] = page_numbers(pages)
                        if "Vol." in publisher:
                            temp["volume"] = re.findall(r'\d+', publisher)[0]
                            publisher = publisher.split(re.findall(r'\d+', publisher)[0])[-1]
                            publisher = re.findall(r"(?<=\s)(.*?)(?=\s)",publisher)
                            publisher = " ".join(publisher)
                            publisher = publisher.rstrip(",")
                            temp["publisher"] = publisher
                        elif publisher:
                            publisher = publisher.rstrip(",")
                            temp["publisher"] = publisher
                        else:
                            pass
                    else:
                        exceptions.append(t)
                    mla_parsed.append([t,temp])
                else:
                    exceptions.append(t)
            else:
                pass
        elif ". " in mla:
            author,rest = mla.split(". ",1)
            temp["author"] = author
            if " Vol." in rest:
                title, rest = rest.split("Vol.",1)
                temp["title"] = title
                volume = re.search('[0-9]+', rest).group()
                temp["volume"] = volume
                rest = rest.split(volume,1)[-1]
                year = re.findall(r"\d{4}", rest)[-1]
                rest = re.sub(year,"",rest)
                rest = re.sub('\W+',' ', rest).strip()
                temp["publisher"] = rest
                temp["year"] = year
                temp["issue"] = ""
            elif " No." in rest:
                title, rest = rest.split(" No.",1)
                temp["title"] = title
                issue = re.search('[0-9]+', rest).group()
                temp["issue"] = issue
                temp["volume"] = ""
                rest = rest.split(issue,1)[-1]
                if rest.strip() == ".":
                    temp["publisher"] = ""
                    temp["year"] = ""
                    continue
                year = re.findall(r"\d{4}", rest)[-1]
                rest = re.sub(year,"",rest)
                rest = re.sub('\W+',' ', rest).strip()
                temp["publisher"] = rest
                temp["year"] = year
            else:
                if "Diss" not in rest:
                    temp["type"] = "book"
                year = re.sub(".*,\\s{0,}(\\w+)\\..*", "\\1", rest)
                rest = rest.split(year,1)[0]
                publisher = re.findall("\\s*([^.]*)", rest)
                tp = [p.strip() for p in publisher if p]
                if tp:
                    title,publisher = tp[:-1],tp[-1]
                    temp["title"] = title
                    temp["year"] = year
                    publisher = publisher.rstrip(",")
                    temp["publisher"] = publisher
                mla_parsed.append([t,temp])
        else:
            exceptions.append(t)

    exceptions_mla = []
    for reference in exceptions:
        mla = reference["MLA"]
        temp = {}
        temp["title"] = ""
        if mla.count('. "') == 1:
            author,rest = mla.split('. "',1)
            title,rest = rest.split('."',1)
            temp["title"] = title
            temp["author"] = author
            if ":" in rest:
                temp["type"] = "article"
                volume = re.findall('\d+', rest.split(":",1)[0])[0]
                temp["volume"] = volume
                journal,pages = rest.split(volume)
                temp["journal"] = journal.strip()
                temp["start pg"],temp["end pg"] = page_numbers(pages)
                exceptions_mla.append([reference,temp])
                continue
            if not re.findall('\d+', rest):
                temp["type"] = "book"
                exceptions_mla.append([reference,temp])
                continue
            else:
                temp["type"] = "misc"
                exceptions_mla.append([reference,temp])
        else:
            title,rest = mla.split('."',1)
            temp["title"] = title[2:]
            year = re.findall(r"\d{4}", rest)[-1]
            temp["year"] = year
            exceptions_mla.append([reference,temp])
    index = mla_parsed+exceptions_mla
    for db in index:
        if "title" in db[-1]:
            if isinstance(db[-1]["title"], list):
                db[-1]['title'] = db[0]["Title"]
                continue
            elif len(db[-1]["title"]) > len(db[0]["Title"]):
                if "\xa0…" in db[0]["Title"]:
                    db[0]["Title"] = db[-1]["title"]
                    continue
    return index

def page_numbers(string):
    # return start,end
    if re.findall("([0-9]+[-]+[0-9]+)", string):
        return re.findall("([0-9]+[-]+[0-9]+)", string)[0].split("-")
    elif re.sub('\W+','', string):
        return re.sub('\W+','', string),re.sub('\W+','', string)
    else:
        return "",""
    return

def clean(mla_parsed):
    ref_index = index_reformat(mla_parsed)
    for r in ref_index:
        if "journal" in r["metadata"]:
            if isinstance(r["metadata"]["journal"],list):
                r["metadata"]["journal"] = r["metadata"]["journal"][0]
            r["metadata"]["journal"] = r["metadata"]["journal"].strip()
    return ref_index

def index_reformat(mla_parsed):
    references = []
    for [identifier,mla] in mla_parsed:
        gs_dict = {}
        newkey = ["title","href","gs-id","mla-citation"]
        for i,key in enumerate(["Title","href","id","MLA"]):
            if key not in identifier:
                identifier[key] = ""
            gs_dict[newkey[i]] = identifier[key]
            identifier.pop(key)
        new_dict = {**identifier,**{"google-scholar":gs_dict},**{"metadata":remove_empty_from_dict(mla)}}
        references.append(new_dict)
    return references

def remove_empty_from_dict(d):
    if type(d) is dict:
        return dict((k, remove_empty_from_dict(v)) for k, v in d.items() if v and remove_empty_from_dict(v))
    elif type(d) is list:
        return [remove_empty_from_dict(v) for v in d if v and remove_empty_from_dict(v)]
    else:
        return d


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'index generator')
    parser.add_argument('--interval', type=int, default=100)
    parser.add_argument('--iteration', type=int, default=5)
    parser.add_argument('-a','--api', nargs='+', help='<Required> Set flag', required=True)
    args = parser.parse_args()
    query_caller(args.interval,args.iteration,args.api)

    with open('gs/fetched.json',"r") as f:
        query = json.load(f)
    query = [dict(y) for y in set(tuple(x.items()) for x in query)]
    gs_data = [q for q in query if len(q) > 2]
    mla_parsed = mla_parser(gs_data)
    references_index = clean(mla_parsed)
    with open("gs/references_index.json", 'w') as f:
        json.dump(references_index, f, ensure_ascii=False, indent=2)

    if os.path.isfile('gs/fetched.json'):
        os.remove('gs/fetched.json')
