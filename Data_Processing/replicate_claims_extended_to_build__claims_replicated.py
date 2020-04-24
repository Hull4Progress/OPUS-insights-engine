#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 03:27:10 2020

@author: rick
"""

from constants_used_for_insights_engine import *
import utils_general
import utils_postgres


from datetime import datetime

import sys

import math

import psycopg2
import psycopg2.extras

# an alternative traditional connection to postgres
# https://stackoverflow.com/questions/27884268/return-pandas-dataframe-from-postgresql-query-with-sqlalchemy
# from sqlalchemy import create_engine


# see https://stackoverflow.com/questions/50174683/how-to-load-data-into-pandas-from-a-large-database
#   even though I am not worried about the large aspect for now
import pandas as pd
# see pandas 1.0.3 api reference guid:
#   https://pandas.pydata.org/pandas-docs/stable/reference/index.html
#   older pandas: see https://pandas.pydata.org/pandas-docs/version/0.13/api.html, search for sql
import pandas.io.sql as psql

import numpy as np

import random

####################################
#
#   importing claims_extended data and adding replicas for subsequent time periods
#
####################################

def pull__claims_extended__table_into_df(db): 
    print('\nEntering pull__claims_extended__table_into_df')
    q = """
          SELECT * from claims_extended
          ORDER BY Claim_Num
        """
    df = psql.read_sql(q, db['conn'])  
    return df


'''
Basic idea is
   claim_num: add i * 100000
   customer_num: add i * 100000
   date_received: add offset of i*17 business days (the original batch has 16 days worth, 
                                                    originally it was Nov 1 to Nov 15,
                                                    but when translated to bus days it is Nov 1 to Nov 22)
   date_follow_up_made: same
   date_all_information_received: same
   date_of_decision__ask_for_nurse_review: same
   date_nurse_decision_made: same
   date_of_decision_after_nurse_review: same
'''
def replicate_claims_extended_df(df_ext, i):
    print('\nEntering function replicate_claims_extended_df for i = ' + str(i))
    df = df_ext.copy(deep=True)
    df['claim_num'] = df.apply(lambda row: row.claim_num + (i*100000),
                                         axis=1)
    df['customer_num'] = df.apply(lambda row: row.customer_num + (i*100000),
                                         axis=1)
    
    for field in ['date_received',
                  'date_follow_up_made',
                  'date_all_information_received',
                  'date_of_decision__ask_for_nurse_review',
                  'date_nurse_decision_made',
                  'date_of_decision_after_nurse_review']:

        df[field] = df.apply(lambda row: utils_general.biz_days_offset(getattr(row, field), i*15),
                             axis=1)
     
    # utils_general.display_df(df)    
    return df
    

####################################
#
#  create and populate claims_extended_replicates
#
####################################

def drop__claims_extended_replicated__table(db):
    print('\nEntered drop__claims_extended_replicated__table')
    q = " DROP TABLE claims_extended_replicated"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_extended_replicated" ')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Failed to drop table "claims_with_durations", perhaps because it did not exist')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """

def create__claims_extended_replicated__table(db):
    print('\nEntered create__claims_extended_replicated__table')
    q = """
          CREATE TABLE claims_extended_replicated as
          SELECT *
          FROM claims_extended
          WHERE Claim_num < 0;
          
          ALTER TABLE claims_extended_replicated
            ADD CONSTRAINT Claim_num_for_replicated_as_KEY 
              PRIMARY KEY (Claim_num);
        """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created the table "claims_extended_replicated" ')
    # except:
    #     # if the attempt to drop table failed, then you have to rollback that request
    #     db['conn'].rollback()
    #     print('Table "claims_extended" did not exist')
    except Exception as e:
            db['conn'].rollback()
            print('  Failed to create table claims_extended_replicated')
            # """
            # to use this part, also adjust the "except" line 3 lines above
            print('  The exception error message is as follows:')
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
            # """

def push_df_list_into__claims_extended_replicated__table(df_list, db):
    print('\nHave entered function push_df_into__claims_with_durations__table')

    table_name = 'claims_extended_replicated'
    for i in range(0,len(df_list)):        
        utils_postgres.load_df_into_table_with_same_columns(df_list[i], db, table_name)
        print('Have inserted records from dataframe with index ' + str(i) + ' into postgres table' )
    

          




####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':

    start_datetime = datetime.now()
    print('\nThis program starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))

    # open postgres connection with mimic database
    db = utils_postgres.connect_postgres()    
    
    df_ext = pull__claims_extended__table_into_df(db)
    
    df_list = []
    df_list.append(df_ext)   # putting the original claims_extended as 1st element of list

    for i in range(1,6):  # in range(1,5):
        df_list.append(replicate_claims_extended_df(df_ext, i))
        
    print('\nThe length of df_list is: ' + str(len(df_list)))

    drop__claims_extended_replicated__table(db)
    create__claims_extended_replicated__table(db)
    push_df_list_into__claims_extended_replicated__table(df_list, db)


    df_concat = pd.concat(df_list)

    print('\nWriting dataframe for claims_extended_replicated into csv file')
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    prefix = OPUS_DATA_OUTPUTS_DIR + timestamp + '__'
    df_concat.to_csv(prefix + 'claims_extended_replicated.csv', index=False)


    
    # close connection to the opus database    
    utils_postgres.close_postgres(db)
 
    
    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = utils_general.truncate(duration_in_s, 2)
    minutes = utils_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    # hours = utils_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    print('\nThe script to augment claims_extended with durations data started running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('The duration in seconds was ' + str(seconds))
    print('The duration in minutes was ' + str(minutes))
    # print('The duration in hours was  ' + str(hours))

