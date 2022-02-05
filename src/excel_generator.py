import pandas.io.formats.excel
import pandas as pd
import json
import xlsxwriter

def excel_main():
    """
    Function to make 7 excel sheets:
    ----------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------

    (1) bank.xlsx -- complete metadata collection; excel format of bank.json
    └──> [Title   Author	   Host Link	Download Link	  En-Title	En-Author	
            Start Page	   End Page	Keywords	Submission Date	    Abstract	References
            References Language	  Listing Index]
    
    (2) collection_stats.xlsx -- descriptive stats on repository
    └──> [Listed Name	Index	No. of Volumes	    No. of Publications	    No. of Articles	
            Earliest Year	Latest Year	    Distinct Authors	Articles w/ Multiple Authors	
            Articles w/ English Info	Articles w/ References	Total No. of References	
            No. of Ko References]

    (3) all_publications.xlsx -- lists every individual journal publication
    └──> [Journal  Index	Subject	    Year	Volume	    No	    Article Count]

    (4) yearly_stats.xlsx -- publication statistics by year
    └──> [Year	Total Journals	    Total Articles	    Korean Journals   English Journals	
            Korean Articles     English Articles	Humanities Journals	  Science Journals	
            Humanities Articles	    Science Articles]

    (5) publications_by_year.xlsx -- compilation of journal releases by year
    └──> [Year	Journal  Name (Index)]
   
    (6) crawled_data.xlsx -- raw data from website crawl; excel format of crawled_info.json
    └──> [Title	    Author	    Location]
    
    (7) crawl_completeness.xlsx -- shows discrepancies between repository vs. website
    └──> [Journal Index	Journal Name	Articles in Collection	Articles Listed on Website	
            Missing Hidden Articles]
   

    ----------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------
    
    To make your own Excel sheets, simply run 
    >>> python src/excel_main.py
    in the command line within the same folder as this repository.
    
    All Excel sheets are updated with information from the latest daily website crawl.
    Last update date is listed in updates.txt. Find legacy versions on [github.com].
    
    """
    
    ## (1) bank.xlsx
    with open('bank.json',"r") as f:
        bank = json.load(f)

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

    ## --------------------------------------------------------

    ## (2) collection_stats.xlsx
    subject_index = {}
    for md5hash,metadata in bank.items():
        subject_index[metadata["Journal"]["Name"]] = metadata["Journal"]["Index"]
    for subject,info in subject_index.items():
        vol_count = []
        no_count = []
        years = []
        no_authors = []
        article_count = 0
        multi_author_count = 0
        yes_eng = 0
        yes_cit = 0
        citation_count = 0
        ko_cit_count = 0
    
        for md5hash,metadata in bank.items():
            if metadata["Journal"]["Name"] == subject:
                years.append(metadata["Journal"]["Year"])
                article_count+=1
                vol_count.append([metadata["Journal"]["Year"],metadata["Journal"]["Volume"]])
                no_count.append([metadata["Journal"]["Year"],metadata["Journal"]["No"]])
                for author in metadata["Author"]:
                    no_authors.append(author)
                if len(metadata["Author"])>1:
                    multi_author_count+=1
                if 'En-Title' in metadata:
                    if metadata['En-Title']:
                        yes_eng+=1
            
                if metadata["References"]:
                    yes_cit+=1
                    citation_count+=len(metadata["References"])
                    ko_cit = 0
                    for i,c in enumerate(metadata["References"]):
                        try:
                            if metadata["References Language"][i] == "ko":
                                ko_cit+=1
                        except IndexError:
                            pass
                    ko_cit_count+=ko_cit
        no_authors = list(set(no_authors))
        vol_count = [list(x) for x in set(tuple(x) for x in vol_count)]
        no_count = [list(x) for x in set(tuple(x) for x in no_count)]
        years = list(set(years))

        subject_index[subject] = [info,len(vol_count),len(no_count),article_count,
                                        min(years),max(years),len(no_authors),multi_author_count,
                                        yes_eng,yes_cit,citation_count,ko_cit_count]

    key_list = ["자연과학","수학","물리학","화학","지구환경과학 및 지질학","생명과학","정보과학","력사학","법률학","력사,법률","철학","경제학","철학,경제학","어문학","NATURAL SCIENCE","SOCIAL SCIENCE"]
    my = sorted(subject_index.items(), key=lambda pair: key_list.index(pair[0]))
    final_stats = {}
    subjects = ['Natural Science','Mathematics','Physics','Chemistry','Environmental Science & Geology','Biology','Informatics','History','Law','History & Law','Philosophy','Economics','Philosophy & Economics','Linguistics','Natural Science (en)','Social Science (en)']
    for i,(subj,stats) in enumerate(my):
        final_stats[subjects[i]] = [subj]+stats
    df = pd.DataFrame.from_dict(final_stats, orient='index',columns=['Listed Name','Index','No. of Volumes','No. of Publications','No. of Articles','Earliest Year','Latest Year','Distinct Authors','Articles w/ Multiple Authors','Articles w/ English Info','Articles w/ References','Total No. of References','No. of Ko References'])
    workbook = pd.ExcelWriter('excel/collection_stats.xlsx', engine='xlsxwriter')
    df.to_excel(workbook, sheet_name='Sheet1')
    workbook.save()    
    ## --------------------------------------------------------
    
    ## (3) all_publications.xlsx
    all_journals = []
    for md5hash,metadata in bank.items():
        j = metadata["Journal"]
        all_journals.append([j["Name"],j["Year"],j["Volume"],j["No"]])

    all_journals = [list(tupl) for tupl in {tuple(item) for item in all_journals }]

    journal_hashes = []
    for journal in all_journals:
        s,y,v,n = journal
        hashes = []
        for md5hash,metadata in bank.items():
            j = metadata["Journal"]
            if j["Name"]==s and j["Year"] == y and j["Volume"] == v and j["No"] == n:
                hashes.append(md5hash)
        journal_hashes.append([journal,hashes])

    journal_collection = []
    for j in journal_hashes:
        journal,hashes = j
        ## keying those hashes to compare information:
        my_collection = []
        for h in hashes:
            title = bank[h]["Title"].strip()
            author = ",".join(bank[h]["Author"])
            my_collection.append([title,author,h])
        journal_collection.append([journal,my_collection])

    row_count = 0
    with xlsxwriter.Workbook('excel/all_publications.xlsx') as workbook:
        worksheet = workbook.add_worksheet()
    
        data_format1 = workbook.add_format({'bold': True, 'fg_color': '#D2F1FF','bottom': 1,'top':1})
        data_format2 = workbook.add_format({'bold': True, 'fg_color': '#A6D7ED','bottom': 1,'left':1,'right':1})

        for journal in journal_collection:
            info,articles = journal
            s,y,v,n = info
            info_str = s+" Vol."+str(v)+" ("+str(y)+") "+ "No."+str(n)

            worksheet.set_row(row_count, cell_format=data_format1)
            worksheet.write(row_count, 0, info_str)
            row_count+=1

            worksheet.set_row(row_count, cell_format=data_format2)
            worksheet.write(row_count, 0, "Title")
            worksheet.write(row_count, 1, "Author")
            worksheet.write(row_count, 2, "Hash")
            for i,m in enumerate(articles):
                worksheet.write_row(row_count+i+1, 0, m)
            worksheet.set_column(0, 1, 35)
            worksheet.set_column(0, 2, 35)
            row_count+=len(articles)
            row_count+=2

    ## --------------------------------------------------------
   
    ## (4) yearly_stats.xlsx
    all_years = []
    for md5hash,metadata in bank.items():
        j = metadata["Journal"]
        all_years.append(j["Year"])
    all_years = list(set(all_years))

    # no of subjects
    # no of articles
    # no of journals

    yearly_stats = []
    for year in all_years:
        subjects = []
        journals = []
        articles = []
        for md5hash,metadata in bank.items():
            j = metadata["Journal"]
            if j["Year"] == year:
                subjects.append(j["Name"])
                journals.append([j["Name"],j["Year"],j["Volume"],j["No"]])
                articles.append(md5hash)
        subjects = list(set(subjects))
        journals = [list(tupl) for tupl in {tuple(item) for item in journals}]
        articles = list(set(articles))

        no_articles = len(articles)
        no_journals = len(journals)
        no_subjects = len(subjects)
        yearly_stats.append([year,no_subjects,no_journals,no_articles])
    yearly_stats = sorted(yearly_stats)
    pandas.io.formats.excel.header_style = None
    df = pd.DataFrame(yearly_stats)
    writer = pd.ExcelWriter('excel/yearly_stats.xlsx', engine='xlsxwriter')
    df.to_excel(writer, 'Sheet1',index=False)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': False,
        'valign': 'top',
        'fg_color': '#D2F1FF',
        'border': 1})

    header_info = ["Year","No of Subjects", "No of Journals","No of Articles"]
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, header_info[col_num], header_format)
    writer.save()

    ## --------------------------------------------------------

    ## (5) publications_by_year.xlsx
    with open('src/journal_info.json',"r") as f:
        journal_info = json.load(f)

    all_years = {}
    for k,v in journal_info.items():
        for year in v[2]:
            all_years[year] = []

    for k,v in journal_info.items():
        for year in v[2]:
            all_years[year].append(v[0]+" ("+k+")")

    all_years = dict(sorted(all_years.items()))

    pandas.io.formats.excel.header_style = None
    df = pd.DataFrame.from_dict(all_years,orient='index')
    writer = pd.ExcelWriter('excel/publications_by_year.xlsx', engine='xlsxwriter')
    df.to_excel(writer, 'Sheet1',index=True)
    workbook = writer.book

    worksheet = writer.sheets['Sheet1']        
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#D2F1FF',
        'border': 1})

    header = ["Year","Journal Name (Index)"]
    for col_num, value in enumerate(df.columns.values):
        try:
            worksheet.write(0, col_num, header[col_num], header_format)
        except IndexError:
            worksheet.write(0, col_num, "", header_format)
            worksheet.write(0, col_num+1, "", header_format)
    writer.save()

    ## --------------------------------------------------------

    ## (6) crawled_info.xlsx
    with open('src/crawled_info.json',"r") as f:
        journal_collection = json.load(f)

    crawled_data = {}
    for i,val in enumerate(journal_collection):
        data = {}
        data["Title"] = val[0]
        data["Author"] = val[1]
        data["Location"] = val[2]
        crawled_data[i] = data

    pandas.io.formats.excel.header_style = None
    df = pd.DataFrame.from_dict(crawled_data,orient='index')
    writer = pd.ExcelWriter('excel/crawled_info.xlsx', engine='xlsxwriter')
    df.to_excel(writer, 'Sheet1',index=False)
    workbook = writer.book

    worksheet = writer.sheets['Sheet1']        
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#D2F1FF',
        'border': 1})
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
    writer.save()

    ## --------------------------------------------------------

    ## (7) crawl_completeness.xlsx
    collection_stats = []
    for k,v in journal_info.items():
        collection_stats.append([int(k),v[0],v[-2],v[-1],v[-1] - v[-2]])
    collection_stats = sorted(collection_stats, key=lambda x: x[0])

    pandas.io.formats.excel.header_style = None
    df = pd.DataFrame(collection_stats)
    writer = pd.ExcelWriter('excel/crawl_completeness.xlsx', engine='xlsxwriter')
    df.to_excel(writer, 'Sheet1',index=False)
    workbook = writer.book

    worksheet = writer.sheets['Sheet1']        
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#D2F1FF',
        'border': 1})
    header = ["Journal Index","Journal Name","Articles in Collection","Articles Listed on Website","Missing Hidden Articles"]
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, header[col_num], header_format)
    writer.save()
    return 


if __name__ == "__main__":
    excel_main()