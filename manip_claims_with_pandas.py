#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 18:50:22 2020

@author: rick
"""

import util_general
from constants_used_for_data_manip import *
import postgres_utilities


from datetime import datetime

import sys

import psycopg2
import psycopg2.extras

# an alternative traditional connection to postgres
# https://stackoverflow.com/questions/27884268/return-pandas-dataframe-from-postgresql-query-with-sqlalchemy
from sqlalchemy import create_engine


# see https://stackoverflow.com/questions/50174683/how-to-load-data-into-pandas-from-a-large-database
#   even though I am not worried about the large aspect for now
import pandas as pd
# see pandas 1.0.3 api reference guid:
#   https://pandas.pydata.org/pandas-docs/stable/reference/index.html
#   older pandas: see https://pandas.pydata.org/pandas-docs/version/0.13/api.html, search for sql
import pandas.io.sql as psql

import random

##########################################

def pull__claims_raw__table_into_df(db):
    print('\nEntering pull__claims_raw__table_into_df')
    q = "SELECT * from claims_raw"
    df = psql.read_sql(q, db['conn'])  
    return df

def compute_hours_worked(claims_per_hour, claim_value_dollars, claim_control_no):
    hours_per_claim = 1/claims_per_hour
    # min claim value is 20000, max claim value is 25000, 
    # goal here is to multipl the hours_per_claim by a number that is between .75 and 1.25
    claim_complexity = .75 + (claim_value_dollars-20000)/5000
    randomized_weighted_hours_per_claim = hours_per_claim * claim_complexity * random.gauss(1, 0.02)
    rounded_rand_weighted_hours_per_claim = round(randomized_weighted_hours_per_claim, 2)
    if claim_control_no < 20:
        print(str(claims_per_hour) + ', ' + str(hours_per_claim) + ', ' \
              + str(claim_value_dollars) + ', ' + str(claim_complexity) + \
                  ', ' + str(hours_per_claim * claim_complexity) + \
                  ', ' + str(rounded_rand_weighted_hours_per_claim))  
    return rounded_rand_weighted_hours_per_claim 

def compute_biz_days_between(date1, date2):
    return util_general.biz_days_between_dates(date1, date2)

def compute_above_5_biz_days(date1, date2):
    duration = util_general.biz_days_between_dates(date1, date2)
    if duration <= 5:
        return 0
    else:
        return 1

def compute_above_10_biz_days(date1, date2):
    duration = util_general.biz_days_between_dates(date1, date2)
    if duration <= 10:
        return 0
    else:
        return 1

def add_columns_to_df(df):  
    print('\nHave entered function to add "hours_worked" to the dataframe.')
    print('We will print, for the first 20 claims, the values of' + \
          '\n   claims_per_hour,' + \
          '\n   hours_per_claim' + \
          '\n   claim_value_dollars' + \
          '\n   claim_complexity (for now, based on claim_value_dollars)' + \
          '\n   hours_per_claim * claim_complexity' + \
          '\n   randomized_weighted_hours_per_claim \n' )

    df['hours_for_this_claim'] = df.apply(lambda row: compute_hours_worked(row.claims_per_hour_per_analyst, 
                                                                           row.claim_value_dollars,
                                                                           row.claim_control_no),
                                          axis=1)
    
    df['nigo_follow_up_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_received,
                                                                         row.date_follow_up_made),
                                    axis=1)    

    df['nigo_requested_to_all_info_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_follow_up_made,
                                                                         row.date_all_information_received),
                                    axis=1)    

    df['nigo_received_to_all_info_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_received,
                                                                         row.date_all_information_received),
                                    axis=1)    

    df['nurse_decision_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_all_information_received,
                                                                         row.date_nurse_decision_made),
                                    axis=1)    

    df['total_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_received,
                                                                         row.date_final_decision_made),
                                    axis=1)
    
    df['over_five_biz_days'] = df.apply(lambda row: compute_above_5_biz_days(row.date_received,
                                                                             row.date_final_decision_made),
                                    axis=1)
    
    df['over_ten_biz_days'] = df.apply(lambda row: compute_above_10_biz_days(row.date_received,
                                                                             row.date_final_decision_made),
                                    axis=1)


    return df
   

def drop__claims_extended__table(db):
    print('\nEntered drop__claims_extended__table')
    q = " DROP TABLE claims_extended"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_extended" ')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table "claims_extended" did not exist')

def create__claims_extended__table(db):
    print('\nEntered create__claims_exztended__table')
    q = """
          CREATE TABLE claims_extended as
          SELECT *
          FROM claims_raw
          WHERE Claim_Control_No < 0;
            
          -- ALTER TABLE claims_extended
          --   ADD bCONSTRAINT Claim_Control_No_as_KEY 
          --     PRIMARY KEY (Claim_Control_No);
          
          ALTER TABLE claims_extended
          ADD COLUMN hours_for_this_claim float8,
          ADD COLUMN nigo_follow_up_biz_days int,
          ADD COLUMN nigo_requested_to_all_info_biz_days int,
          ADD COLUMN nigo_received_to_all_info_biz_days int,
          ADD COLUMN nurse_decision_biz_days int,
          ADD COLUMN total_biz_days int,
          ADD COLUMN over_five_biz_days int,
          ADD COLUMN over_ten_biz_days int
        """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created the table "claims_extended" ')
    # except:
    #     # if the attempt to drop table failed, then you have to rollback that request
    #     db['conn'].rollback()
    #     print('Table "claims_extended" did not exist')
    except Exception as e:
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

def push_df_into__claims_extended__table(df, db_conn):
    print('\nHave entered function push_df_into__claims_extended__table')

    table_name = 'claims_extended'
    postgres_utilities.load_df_into_table_with_same_columns(table_name, df, db_conn)
    
    '''
    table = 'claims_extended'
    if len(df) > 0:
        df_columns = list(df)
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

        #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table,columns,values)

        psycopg2.extras.execute_batch(db['cursor'], insert_stmt, df.values)
        db['conn'].commit()
    '''





        
####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    
    # print('\nWARNING: You may need to uncomment some commands from the main program of assemble_triples_table.py (and/or in some of the invoked scripts) in order to get the behaviors that you want.')
    
    start_datetime = datetime.now()
    print('\nThis program starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    
    # open postgres connection with mimic database
    db_conn = postgres_utilities.connect_postgres()

    

    df = pull__claims_raw__table_into_df(db_conn)
    # print(df)
    # print(list(df.columns.values))
    
    df = add_columns_to_df(df)
    print(df)
    print(list(df.columns.values))
    
    drop__claims_extended__table(db_conn)
    create__claims_extended__table(db_conn)
    
    # using psycopg2 and psycopg2.extras:
    postgres_utilities.load_df_into_table_with_same_columns(df, db_conn, 'claims_extended')

    # close connection to the mimic database    
    postgres_utilities.close_postgres(db_conn)

    
    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = util_general.truncate(duration_in_s, 2)
    minutes = util_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    hours = util_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    print('\nThe manip_claims_with_pandas script starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('The duration in seconds was ' + str(seconds))
    print('The duration in minutes was ' + str(minutes))
    print('The duration in hours was  ' + str(hours))
       

    
