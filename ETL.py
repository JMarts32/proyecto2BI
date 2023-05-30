from sqlalchemy import create_engine
import pandas as pd
import os

pwd = "postgres"
uid = "postgres"
server = 'localhost'
database = 'asma'
port = '5432'
dir = r'./data/ETL'

def extract():
    try:
        directory = dir
        for filename in os.listdir(directory):
            file_wo_ext = os.path.splitext(filename)[0]
            if filename.endswith(".csv"):
                f = os.path.join(directory, filename)

                if os.path.isfile(f):
                    df = pd.read_csv(f, sep=',')
                    load(df, file_wo_ext)
    except Exception as e:
        print(e)


def load(df, table_name):
    try:
        rows_imported = 0
        engine = create_engine('postgresql://' + uid + ':' + pwd + '@' + server + ':' + port + '/' + database)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}...')
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print('Data loaded successfully')
    except Exception as e:
        print(e)
            
try:
    df = extract()
except Exception as e:
    print(e)