import sqlite3
import pandas as pd
import sys

class CreateTable:
    
    def __init__(self, cursr, table_name, table_defn):
        self.cursr          = cursr
        self.table_name     = table_name
        self.table_defn     = table_defn

    def create_table(self):
        try:
            self.cursr.execute(f'''CREATE TABLE {self.table_name} {tuple(self.table_defn)}''')
            print ('Table Created Sucessfully - {} '.format(self.table_name))
        except :
            print('Table Creation Failed - {}'.format(sys.exc_info()[0]))
        return

class InsertIntoTable:
    
    def __init__(self, conn, cursr, table_name):
        self.conn           = conn
        self.cursr          = cursr
        self.table_name     = table_name

    def insert_record(self, record):
        try:
            self.record = record
            self.cursr.execute(f'''INSERT INTO {self.table_name} VALUES {self.record}''')
            self.conn.commit()
            print ('Record Updated Sucessfully - {} '.format(self.record))
        except :
            print ('Record Update Failed - {}'.format(sys.exc_info()[0]))
        return

class FetchFromTable:
    
    def __init__(self, conn):
        self.conn = conn
    
    def fetch_data(self, query):
        self.query = query        
        query_result_df = pd.read_sql(query, self.conn)
        return query_result_df

class DropFromTable:
    
    def __init__(self, conn, cursr, drop_query):
        self.conn           = conn
        self.cursr          = cursr
        self.drop_query     = drop_query

    def drop_record(self):
        try:
            self.cursr.execute(self.drop_query)
            self.conn.commit()
            print ('Record deleted sucessfully!')
        except :
            print ('Record deletion Failed !')
        return