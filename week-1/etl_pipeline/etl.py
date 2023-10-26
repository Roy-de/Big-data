from sqlalchemy import create_engine
import pandas as pd
import os
import pyodbc

#Set password
pwd = 'Oppoa3s2019#'
uid = 'sa'

driver = "ODBC Driver 17 for SQL Server"
server = "localhost"
database = "AdventureWorksDW2019"
def extract():
    sql_query = """select  t.name as table_name
        from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales')"""
    try:
        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};"
        src_conn = pyodbc.connect(conn_str)
        print("Established connection...")
        src_cursor = src_conn.cursor()
        # execute query
        src_cursor.execute(sql_query)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            #query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

#load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{"postgres"}:{"7934"}@{server}:5432/adventureworks')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))