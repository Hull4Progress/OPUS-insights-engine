#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 15:19:50 2020

@author: rick
"""

from constants_used_for_insights_engine import *
import utils_general

import sys

# see http://initd.org/psycopg/docs/usage.html
import psycopg2
# from psycopg2.extras import RealDictCursor
# see https://stackoverflow.com/questions/10252247/how-do-i-get-a-list-of-column-names-from-a-psycopg2-cursor
#  about half way down
import psycopg2.extras

# see https://stackoverflow.com/questions/50174683/how-to-load-data-into-pandas-from-a-large-database
#   even though I am not worried about the large aspect for now
import pandas as pd
# see pandas 1.0.3 api reference guid:
#   https://pandas.pydata.org/pandas-docs/stable/reference/index.html
#   older pandas: see https://pandas.pydata.org/pandas-docs/version/0.13/api.html, search for sql
import pandas.io.sql as psql

# see https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_introduction.htm
from sqlalchemy import create_engine

#############################################
#
#
#
#############################################

def connect_postgres():
    try:
        print('\nAttempting to connect to the opus database using psycopq2')
        conn = psycopg2.connect(POSTGRES_SIGN_IN_CREDENTIALS_FOR_PSYCOPG2)
        cursor = conn.cursor()
        print('Successfully connected to the OPUS database, and created a cursor for it')
        db = {}
        db['conn'] = conn
        db['cursor'] = cursor
        # set search path to opus schema
        cursor.execute("set search_path to opus")
        conn.commit()
        print('Set search_path to "opus" and committed')
        return db
    except:
        print ("\nI am unable to connect to the database")
        print('\nExiting program because failed to connect to opus database\n')
        sys.exit()

'''
def add_indexes_to_mimiciii_tables(db):
    print('\nEntered add_indexes_to_mimiciii_tables')
    q = {}
    q['chartevents__itemid'] = """ 
           create index chartevents__itemid
           on chartevents(itemid)
         """
    q['chartevents__hadm_id'] = """ 
           create index chartevents__hadm_id
           on chartevents(hadm_id)
         """
    q['chartevents__subject_id'] = """ 
           create index chartevents__subject_id
           on chartevents(subject_id)
         """
    q['inputevents_mv__subj_item_starttime'] = """
           create index inputevents_mv__subj_item_starttime 
           on inputevents_mv(subject_id, itemid, starttime)
         """
    q['labevents__subject_id'] = """ 
           create index labevents__subject_id
           on labevents(subject_id)
         """
    q['d_items__category'] = """ 
           create index d_items__category
           on d_items(category)
         """
    for key in q:
        try:
            db['cursor'].execute(q[key])
            db['conn'].commit()
            print('  Successfully created index ' + key)
        except: #Exception as e:
            db['conn'].rollback()
            print('  Failed to create index ' + key + ', probably because it already exists')
            """
            # to use this part, also adjust the "except" line 3 lines above
            print('  The exception error message is as follows:')
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
            """

    print('Added indexes for mimiciii tables (if needed), including a commit')
'''


def close_postgres(db):
    try:
        db['cursor'].close()
        db['conn'].close()
        print('\nHave closed the cursor and connection to the opus database')
    except:
        print('\nInput to the close_postgres function call is not holding a postgres connection and cursor')



# following about the third comment down in 
# https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
# Assumes that:
#     table_name has been defined (and typically, is empty)
#     table_name column names are same as df (but I think can be in different order)
#     table_name column data types match with df column data types
#     if table_name does not include a schema name, then already a "set search_path to ...' has been invoked
#     db is a structure with 'conn' and 'cursor', created using connect_postgres() above
def load_df_into_table_with_same_columns(df, db, table_name):
    print('\nEntering routine will load a dataframe into the postgres table ' + table_name)
    if len(df) > 0:
        df_columns = list(df)
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

        #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table_name,columns,values)
        
        try:
            psycopg2.extras.execute_batch(db['cursor'], insert_stmt, df.values)
            db['conn'].commit()
        except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
            db['conn'].rollback()
            print('  Failed to create index ' + key + ', probably because it already exists')
            # """
            # to use this part, also adjust the "except" line 3 lines above
            print('  The exception error message is as follows:')
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
            # """
            
    
def export_table_to_csv(table, db, timestamp):
    print('\nEntering function to print table "' + table + '" to csv file')
    q = "select * from " + table
    
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(q)
    # timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    dirName = OPUS_DATA_OUTPUTS_DIR 
    fileName = timestamp + '_' + table + '.csv'
    
    with open(dirName + fileName, 'w') as f:
        db['cursor'].copy_expert(outputquery, f)
    f.close()
    print('   Wrote csv file ' + dirName + fileName )
    # util_general.print_current_time()
    
def export_cube_query_to_csv(columns_list, q, db, timestamp):
    print('\nEntering function to print the output of a cube query to a csv file')

    columns_str = ''
    if len(columns_list) == 0:
        columns_str = '__empty_column_list'
    else: 
        for i in range(0, len(columns_list)):
            columns_str = columns_str + '__' + columns_list[i]
    
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(q)
    # timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    dirName = OPUS_DATA_OUTPUTS_DIR 
    # column_list
    fileName = timestamp + '__claims_grouped_by' + columns_str + '.csv'
    
    with open(dirName + fileName, 'w') as f:
        db['cursor'].copy_expert(outputquery, f)
    f.close()
    print('Wrote csv file:' )
    print('----------------------------------------------------')
    print(fileName)
    print('----------------------------------------------------')
    print('into the directory ' + dirName)
    # util_general.print_current_time()
    
    
    
    
    
# illustration of using sqlalchemy
def testdb(db_eng):
    print('\nEntered the function testdb')
    q1 = "set search_path to opus"
    q2 = "CREATE TABLE IF NOT EXISTS films  (title text, director text, year text)"
    q3 = """
           INSERT INTO opus.films (title, director, year)
           VALUES ('Doctor Strange', 'Scott Derrickson', '2016')
	 """
    with db_eng.connect() as conn:
        conn.execute(q1)
        conn.execute(q2)
        conn.execute(q3)





#######
#
#  emain
#
#######

if __name__ == '__main__':
    
    pass

    # test of using sqlalchemy
    # db_eng = create_engine(DB_ADDRESS_FOR_SQLALCHEMY)
    # testdb(db_eng)
