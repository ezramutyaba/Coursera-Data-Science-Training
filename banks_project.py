import glob
from msilib import add_data 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

log_file = "code_log.txt" 
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'

db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = 'Largest_banks_data.csv'
df = pd.DataFrame(columns=["Name","MC_USD_Billion"])
table_attribs = 'tbody'


## Function to log everything
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

## Function to extract using beautifulsoup
def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    count = 0
    tables = data.find_all(table_attribs)
    rows = tables[0].find_all('tr')
    ext_df = pd.DataFrame(columns=["Name","MC_USD_Billion"])

    for row in rows:
        if count<50:
            col = row.find_all('td')
            if len(col)!=0:
                data_dict = {"Name": col[1].text.strip(),
                            "MC_USD_Billion": float(col[2].contents[0].strip())}
                df1 = pd.DataFrame(data_dict, index=[0])
                ext_df = pd.concat([ext_df,df1], ignore_index=True)
                count+=1
        else:
            break
    return ext_df

## Function to transform data
def transform(df):
    df['MC_GBP_Billion'] = round(df.MC_USD_Billion * 0.8,2)
    df['MC_EUR_Billion'] = round(df.MC_USD_Billion * 0.93,2)
    df['MC_INR_Billion'] = round(df.MC_USD_Billion * 82.95,2)
    return df

## Function to send to CSV
def load_to_csv(csv_path):
    df.to_csv(csv_path)
    
def load_to_db(db_name,table_name,df):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def load_from_db(db_name,table_name):
    conn = sqlite3.connect(db_name)
    query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
    query_output = pd.read_sql(query_statement, conn)
    print(query_statement)
    print(query_output)
    conn.close()


print(df)


# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
df2 = extract(url, table_attribs)
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
print(df2) 
df = transform(df2)
print("Transformed Data") 
print(df) 
load_to_csv(csv_path)
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_to_db(db_name,table_name,df)
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 

# Log the beginning of the Loading from DB 
log_progress("Load phase Started") 
load_from_db(db_name,table_name)
 
# Log the completion of the Loading from DB 
log_progress("Load phase Ended") 

# Log the completion of the ETL process 
log_progress("ETL Job Ended") 



#