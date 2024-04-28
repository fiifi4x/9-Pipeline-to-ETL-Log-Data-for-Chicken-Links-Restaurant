import os
import pandas as pd
import psycopg2
import json
import ast                          # Abstract syntax tree used for obtaining actual python objects from string
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import dotenv_values
dotenv_values()

def get_database_connection():
    # load the .env files and save as config variable
    config = dotenv_values(".env")      # so config now has all the key values from the .env. # though config is a variable, it is an industry standard
                                        # Then call the config variable which containes all the .env values in dictionaries
    db_name = config['DB_NAME']
    db_host = config["HOST"]
    db_port = config["PORT"]
    db_username = config["DB_USER_NAME"]
    db_password = config["DB_PASSWORD"]
    return create_engine(f'postgresql+psycopg2://{db_name}:{db_port}@{db_host}:{db_password}:{db_username}')

def extract():
    parsed_dicts = []
    with open('new.json', 'r') as freshlyLoaded:
        for each_line in freshlyLoaded:
            try:
                each_line = ast.literal_eval(each_line) 
                parsed_dicts.append(each_line)
            except (SyntaxError, ValueError):
                # Handle the case where `each_line` is not a valid literal
                each_line = "literal cannot be parsed"  # Or another appropriate action
    df = pd.DataFrame(parsed_dicts) 
    return df       

def df_transform_and_write_csv():
    df = extract()
    df["event_date"] = pd.to_datetime(df["event_date"], format = "%Y%m%d").dt.date
    df.to_csv(f'record_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv', index=False)
    print('df written to csv - staging area')

def csvReloadTransform():
    current_csv = pd.read_csv(f'record_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv') #file re-read so date format is in string again
    #convert the event_date to date time, its now a string
    # current_csv['event_date'] = pd.to_datetime(current_csv["event_date"]).dt.date
    # current_csv.info()
    # print(current_csv)
    return current_csv

def postgres_load():
    csv_to_load = csvReloadTransform()
    table_name = "first_page"                                   
    csv_to_load.to_sql(table_name, con=get_database_connection(), if_exists='replace', index=False)
    print('File_Sent_To_Postgres_Database')
    
def postgres_read():
    table_name = "first_page"
    db_table = pd.read_sql(table_name, con=get_database_connection())
    print(db_table)

# extract()
# df_transform_and_write_csv()
# csvReloadTransform()
# postgres_load()
#postgres_read()

# method 1
# query = '''select max(event_date) from first_page'''
# last_updated = pd.read_sql(query, con= get_database_connection()).values[0][0]
# print(last_updated)

# method 2
all_data = pd.read_sql("(SELECT MAX(event_date) FROM env_trials.public.first_page)", con=get_database_connection()).values[0][0]
print(all_data)




# new_data = new_data[new_data['date'] > last_updated]
# new_data.to_sql('first_page', con= get_database_connection(), if_exists = 'append', index = False)
# print('Data loaded successfully')

#QueryWithCSV()





