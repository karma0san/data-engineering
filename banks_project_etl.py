import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime


def extract(url, table_attributes):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attributes)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    data_list = []

    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            links = col[1].find_all('a')
            if len(links)!= 0:
                data_dict = {'Name': links[1].contents[0], 'MC_USD_Billion': col[2].contents[0][:-1]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
        
    return df

def transform(df):
    exchange_df = pd.read_csv(exchange_rate)
    # print(exchange_df.head())

    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
        
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * float(exchange_df['Rate'][1]),2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * float(exchange_df['Rate'][0]),2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * float(exchange_df['Rate'][2]),2)

    return df

def load_to_csv(df, csv_file):
    df.to_csv(csv_file)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attributes = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_file = './Largest_banks_data.csv'
log_file = './code_log.txt'



log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attributes)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
# print(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_file)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()