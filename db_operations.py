import pandas as pd
import datetime
from create_db import CreateTable, InsertIntoTable, FetchFromTable, DropFromTable
import sqlite3

def create_table(cursr, table_name):
    table_defn = {  'Employee_ID'           : 'INTEGER PRIMARY KEY', 
                    'Employee_Name'         : 'TEXT NOT NULL',
                    'Phone'                 : 'TEXT NOT NULL',
                    'Email'                 : 'TEXT NOT NULL',
                    'Update_TimeStamp'      : 'TEXT NOT NULL'
                }
    create_table = CreateTable(cursr, table_name, table_defn)
    
    create_table.create_table()
    
    return

def batch_update_table(df, conn, conn_crsr, table_name):
    
    update_rec_into_table = InsertIntoTable(conn, conn_crsr, table_name)

    for i in range(df.shape[0]):
        update_rec_into_table.insert_record(tuple(df.loc[i]))    
        print(f"""{df.shape[0]} records updated sucessfully""")

    return

def single_rec_update(updt_rec, conn, conn_crsr, table_name):
    
    update_rec_into_table = InsertIntoTable(conn, conn_crsr, table_name)
    update_rec_into_table.insert_record(updt_rec)
    
    return

def fetch_table(query, conn):
    fetch_data_from_table = FetchFromTable(conn)
    qry_result = fetch_data_from_table.fetch_data(query)
    return qry_result

def drop_table(conn, conn_crsr, drop_query):
    drop_data_from_table = DropFromTable(conn, conn_crsr, drop_query)
    drop_data_from_table.drop_record()
    return

if __name__ == '__main__':

    conn        = sqlite3.connect('./db/employee.db')
    conn_crsr   = conn.cursor()
    table_name  = 'employee_tbl'
    
    # Create Table
    create_table(conn_crsr, table_name)
    
    # Add/Update Single Record
    updt_rec = (111111, 'Jagannath Banerjee', '123-456-7890', 'test@gmail.com', str(datetime.datetime.now().date()) + ' ' + str(datetime.datetime.now().time()))

    single_rec_update(updt_rec, conn, conn_crsr, table_name)

    # Add/Update Batch Records
    df = pd.read_csv('./db/upload_data.csv')
    df = df.drop(columns='Update_TimeStamp')
    df['Update_TimeStamp'] = str(datetime.datetime.now().date()) + ' ' + str(datetime.datetime.now().time())
    batch_update_table(df, conn, conn_crsr, table_name)

    # Fetch from table
    query = f'''SELECT * FROM {table_name}'''
    qry_result = fetch_table(query, conn)
    print(qry_result)

    # Drop from table
    drop_query = f"""DELETE FROM {table_name} where Employee_ID=111111"""
    drop_table(conn, conn_crsr, drop_query)

    # Validate after Drop
    query = f'''SELECT * FROM {table_name}'''
    qry_result = fetch_table(query, conn)
    print(qry_result)