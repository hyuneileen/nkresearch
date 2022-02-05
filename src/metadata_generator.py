import json
from metadata_extract import ko_main,en_main
import hashlib
import argparse
import timeit
import datetime
import logging
from custom_logger import CustomFormatter, progressbar
from excel_generator import excel_main
import pandas as pd
import pandas.io.formats.excel
import io
import csv
import pickle
import os

logger = logging.getLogger("Data Generator")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


def metadata_main(file):
    """
    Compilation function that synthesizes missing metadata fields in bank.
    
    Creates metadata classes:   En-Title, En-Author, Start Page, End Page, Keywords
                                Submission Date, Abstract, References, References Language,
                                Listing Index
    
    All but the last uses text extracted directly from the sourcecode of the PDF,
    which the script "metadata_extract.py" handles. All Excel sheets in folder "/metadata" 
    are also created here by by calling script "excel_main.py". See each script 
    for more details. The Listing Index is generated by reversing the MD5 hash.


    How to use 
    ----------------------
    Enter
    >>> python src/metadata_generator.py 
    in the command line within the same folder as this repository.
    
    In order to run the metadata function using the predefined extracted text streams,
    (under folder /streams), enter:
    >>> python src/metadata_generator.py --file csv
   
    Extracting the streams takes up the vast majority of time; this reduces
    time by %90. You must have the /streams folder downlaoded from the root repo.

    Output
    ----------------------
    Completed bank.json & all Excel sheets under folder "/metadata"

    """
    
    with open('src/crawled_info.json',"r") as f:
        crawled_info = json.load(f)
     
    # ------------------------------------------------------------------
    logger.info("Making metadata storage framework in bank.json...")
    bank, directory_data = crawl_processor(crawled_info)
    
    logger.info("Depository structure formed.")
    # ------------------------------------------------------------------
    bank = bank_compiler(file,bank,directory_data)
    # ------------------------------------------------------------------
    logger.info("Cleaning text streams into txt files in /corpus...")
    corpus_maker(directory_data,bank)
    # ------------------------------------------------------------------

    with open('src/directory_mapping.json', 'w') as f:
        json.dump(directory_data, f)
    
    with open('bank.json', 'w') as f:
        json.dump(bank, f, ensure_ascii=False, indent=2)
    
    # ------------------------------------------------------------------
    logger.info("bank generated. Now parsing data into excel files...")
    metadata(bank)
    excel_main()
    return 


def corpus_maker(directory_data,bank):
    for md5hash,j_directory in directory_data.items():
        directory = j_directory.split("journals")[1]
        extracted_path = "streams"+directory+md5hash+".csv"
        
        ## open pickled csv file
        with open(extracted_path, 'rb') as f:
            text_tuples = pickle.load(f)
        
        ## quick processing
        text_only = [c[2] for c in text_tuples]
        text_only = " ".join(text_only)
        text_only = text_only.replace("\n"," ")
        text_only = text_only.replace("  "," ")

        ## make directory
        corpus_path = "corpus"+directory
        if not os.path.exists(corpus_path):
            os.makedirs(corpus_path) 

        with open(corpus_path+md5hash+".txt", "w") as text_file:
            text_file.write(text_only)
        
    corpus_compiler(directory_data,bank)
    return


def corpus_compiler(directory_data,bank):
    comp = {}
    for md5hash,j_directory in directory_data.items():
        directory = j_directory.split("journals")[1]

        corpus_path = "corpus"+directory
        with open(corpus_path+md5hash+".txt", "r") as text_file:
            corpus = text_file.read()
        
        temp = {}
        temp["Title"] = bank[md5hash]["Title"]
        temp["Text"] = corpus

        comp[md5hash] = temp

    ## make excel
    pandas.io.formats.excel.header_style = None
    df = pd.DataFrame.from_dict(comp)
    df =df.T
    writer = pd.ExcelWriter('corpus/corpus.xlsx', engine='xlsxwriter')
    
    df.to_excel(writer, 'Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']        
    
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': False,
        'valign': 'top',
        'fg_color': '#d5fdfa',
        'border': 1})
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num+1, value, header_format)
    writer.save()
    return


def metadata(bank):
    # (1) another json copy under /validation for jupyter notebook
    with open('validation/bank.json',"w") as f:
        json.dump(bank, f, ensure_ascii=False, indent=2)

    ## (2) readable csv: metadata/bank.csv
    with open('metadata/bank.csv', 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in bank.items():
            writer.writerow([key, value])

    ## (3) pickle: metadata/bank.pkl
    with open('metadata/bank.pkl', 'wb') as file:
        pickle.dump(bank, file)
    
    ## (4) excel: metadata/bank.xlsx
    for k,v in bank.items():
        for kk,vv in v.items():
            if not vv:
                v[kk] = None

    pandas.io.formats.excel.header_style = None
    df = pd.DataFrame.from_dict(bank)
    df =df.T
    writer = pd.ExcelWriter('excel/bank.xlsx', engine='xlsxwriter')
    df.to_excel(writer, 'Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']        

    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': False,
        'valign': 'top',
        'fg_color': '#D2F1FF',
        'border': 1})
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num+1, value, header_format)
    writer.save()

    multiindex(bank)
    return



def multiindex(bank):
    df = pd.DataFrame.from_dict(bank, orient='index')
    new_d = [{**bank[k],**{"MD5":k},**bank[k]["Journal"],**bank[k]["Links"]} for k in (list(bank.keys()))]
    df = pd.DataFrame(new_d)
    df = df.set_index(['Language','Name','Index','Volume','Year','No']).sort_index()
    del df['Journal']
    del df['Links']
    cols = ['Start Page','End Page']
    for col in cols:
        df[col] = df[col].apply(lambda x: int(x) if x == x else "")
    
    # to readable csv
    df.to_csv("metadata/multiindex.csv")

    # to pickle
    df.to_pickle('metadata/multiindex.pkl')

    # to excel
    workbook = pd.ExcelWriter('metadata/multiindex.xlsx', engine='xlsxwriter')
    df.to_excel(workbook, sheet_name='Sheet1')
    workbook.save()

    # txt file of dataframe dimensions
    buffer = io.StringIO()
    df.info(buf=buffer)
    s = buffer.getvalue()
    with open("metadata/multiindex_info.txt", "w", encoding="utf-8") as f:
        f.write(s)
    return



def bank_compiler(file, bank, directory_data):
    start_time = timeit.default_timer()

    md5hash_to_content_dict = content_generator(file,directory_data)

    total_time = timeit.default_timer() - start_time
    logger.info("Finished synthesizing metadata from PDF content. Operation took  %s.",str(datetime.timedelta(seconds=total_time)))
    # ------------------------------------------------------------------
    ## now adding listing index...
    logger.info("Now finding publication indices...")

    md5hash_to_content_dict = index_generator(md5hash_to_content_dict)
    # ------------------------------------------------------------------
    ## completing bank

    logger.info("Finishing generating bank...")
    for md5hash,metadata in bank.items():
        bank[md5hash] = dict(metadata,**md5hash_to_content_dict[md5hash])
    return bank



def crawl_processor(crawled_info):
    bank = {}
    directory_mapping = {}

    def ko_volume(year):
        return year-1954

    def en_volume(year,index):
        if year == 2019:
            return 5
        if index == 2 and year == 2014:
            return 2
        elif index == 1:
            return year-2011

            
    with open('src/journal_info.json',"r") as f:
        journal_info = json.load(f)

    for i,c in enumerate(crawled_info):
        temp = {}
        ## generating bank
        ## Title, Author, Host Link, Download Link, Hash
        ## (2) journal info 
        if not c:
            del crawled_info[i]
            continue
        else:
            title = c[0].strip()
            author = c[1]
            author = author.replace("and",",")
            author = [a.strip() for a in author.split(",")]
            # journal info
            j_info = {}
            journal = c[2].split("/")
            lang = journal[2]
            journal_index,year,no,md5hash = journal[-4:]

            if lang == "ko":
                volume = ko_volume(int(year))
            if lang == "en":
                volume = en_volume(int(year),int(journal_index))

            j_info["Name"] = journal_info[str(journal_index)][0]
            j_info["Index"] = int(journal_index)
            j_info["Volume"] = int(volume)
            j_info["Year"] = int(year)
            j_info["No"] = int(no)
            j_info["Language"] = lang
            
            # generate links
            links = {}
            viewer = "http://www.ryongnamsan.edu.kp/univ/plugins/pdfviewer/web/viewer.html?file="+md5hash+lang+"j"
            host = "http://www.ryongnamsan.edu.kp"+c[2]
            download = "http://www.ryongnamsan.edu.kp/univ/"+lang+"/research/journals/paper/"+md5hash
            links["Viewer"] = viewer
            links["Host"] = host
            links["Download"] = download
            
            temp["Title"] = title
            temp["Author"] = author
            temp["Journal"] = j_info
            temp["Links"] = links
            bank[md5hash] = temp

            ## map hash to directory for convinience
            directory = c[2].split("/")[-5:-1]
            directory.insert(1, lang)
            directory = "/".join(directory)+"/"
            directory_mapping[md5hash] = directory
    return bank, directory_mapping


def parse_path(file,md5hash,directory):
    if file == "pdf":
        PDF_path = directory+md5hash+".pdf"
        return PDF_path
    if file == "csv":
        csv_directory = directory.split("journals")[1]
        csv_directory = "streams/"+csv_directory+"/"
        csv_path = csv_directory+md5hash+".csv"
        return csv_path


def content_generator(file,directory_data):
    logger.info("PDF directory generated. Now synthesizing metadata from PDF sourcecode.")

    if file == "pdf":
        logger.info("Generating text tuples from PDF sourcecode...")
        logger.info("Saving text tuples in folder /streams...")
        
    if file == "csv":
        logger.info("Loading text tuples from folder /streams...")
        logger.info("ETA is 7 minutes")

    md5hash_to_content_dict = {}

    for md5hash,directory in progressbar(directory_data.items(), "Generating metadata: ", 40):
        lang = directory.split("/")[1]
        path = parse_path(file,md5hash,directory)

        if lang == "ko":
            ### content_dict = {"En-Title":EnTitle, "En-Author":EnAuthor, 
            ###                 "Start Page":StartPage, "End Page":EndPage, "Keywords":Keyword,
            ###                 "Submission Date":SubDate, "Abstract":EnAbstract, "References":References}

            content_dict = ko_main(path)
            md5hash_to_content_dict[md5hash] = content_dict

        if lang == "en":
            ### content_dict = {"Start Page":StartPage, "End Page":EndPage, "Keywords":Keyword,
            ###                  "Abstract": Abstract, "References":References, "References Language:CitLang"}

            content_dict = en_main(path)
            md5hash_to_content_dict[md5hash] = content_dict
    
    return md5hash_to_content_dict


def index_generator(md5hash_to_content_dict):

    def get_md5(text):
        m = hashlib.md5()
        m.update(text.encode('UTF-8'))
        return m.hexdigest()
    
    ## store reversed dict for efficiency
    md5_to_integer = {}
    for i in list(range(1,8000)):
        md5 = get_md5(str(i))
        md5_to_integer[md5] = i

    for md5hash,content_dict in md5hash_to_content_dict.items():
        md5hash_to_content_dict[md5hash] = dict(content_dict,**{"Listing Index":md5_to_integer[md5hash]})

    return md5hash_to_content_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'metadata generator')
    parser.add_argument('-f','--file', type=str, default='pdf')
    args = parser.parse_args()
    metadata_main(args.file)