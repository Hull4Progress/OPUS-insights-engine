#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 23:04:38 2020

@author: rick
"""

from constants_used_for_insights_engine import *
import utils_general
import utils_postgres


from datetime import datetime

import sys

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

import random

####################################
#
#   importing durations data
#
####################################

def drop__diagnosis_duration_parameters__table(db):
    print('\nEntered drop__diagnosis_duration_parameters__table')
    q = " DROP TABLE diagnosis_duration_parameters"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "diagnosis_duration_parameters" ')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Failed to drop table "diagnosis_duration_parameters", perhaps because it did not exist')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """

def create__diagnosis_duration_parameters__table(db):
    print('\nEntered create__diagnosis_duration_parameters__table')
    q = """
          CREATE TABLE diagnosis_duration_parameters (              
             Diagnosis	varchar,
             Min_Duration_Days	int4,
             Max_Duration_Days	int4,
             Rec_Touch_Number int4,
             Percent_5_Touches int4,
             Percent_4_Touches int4,
             Percent_3_Touches int4,	
             Percent_2_Touches int4,	
             Percent_1_Touches int4,	
             Percent_0_Touches int4 
          )
	     """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created table "diagnosis_duration_parameters", including a commit')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Attempt to create table "diagnosis_duration_parameters" has failed')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """

    
def import__diagnosis_duration_parameters_csv__into_postgres(db):
    print('\nEntered import__diagnosis_duration_parameters_csv__into_postgres')

    q = """
         copy diagnosis_duration_parameters (
           Diagnosis,
           Min_Duration_Days,
           Max_Duration_Days,
           Rec_Touch_Number,
           Percent_5_Touches,
           Percent_4_Touches,
           Percent_3_Touches,
           Percent_2_Touches,
           Percent_1_Touches,
           Percent_0_Touches
          )
          from stdin with
          csv
          header
          delimiter as ','
        """
    
    fileName = DIAGNOSIS_DURATION_PARAMETERS
    
    f = open(fileName, 'r')
    db['cursor'].copy_expert(sql=q, file=f)
    f.close()
    db['conn'].commit()

def pull__diagnosis_duration_parameters__table_into_df(db):
    print('\nEntering pull__diagnosis_duration_parameters__table_into_df')
    q = """
          SELECT * from diagnosis_duration_parameters
          ORDER BY diagnosis
        """
    df = psql.read_sql(q, db['conn'])  
    return df

####################################
#
#   importing claims analyst paraeters data
#
####################################

def drop__claims_analyst_parameters__table(db):
    print('\nEntered drop__claims_analyst_parameters__table')
    q = " DROP TABLE claims_analyst_parameters"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_analyst_parameters" ')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Failed to drop table "claims_analyst_parameters", perhaps because it did not exist')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """

def create__claims_analyst_parameters__table(db):
    print('\nEntered create__claims_analyst_parameters__table')
    q = """
          CREATE TABLE claims_analyst_parameters (              
              Analyst varchar,
              Duration_Touch_Compliance float8
          )
	     """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created table "claims_analyst_parameters", including a commit')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Attempt to create table "claims_analyst_parameters" has failed')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """

    
def import__claims_analyst_parameters_csv__into_postgres(db):
    print('\nEntered import__dclaims_analyst_parameters_csv__into_postgres')

    q = """
         copy claims_analyst_parameters (
              Analyst,
              Duration_Touch_Compliance
          )
          from stdin with
          csv
          header
          delimiter as ','
        """
    
    fileName = CLAIMS_ANALYST_PARAMETERS
    
    f = open(fileName, 'r')
    db['cursor'].copy_expert(sql=q, file=f)
    f.close()
    db['conn'].commit()

def pull__claims_analyst_parameters__table_into_df(db):
    print('\nEntering pull__diagnosis_duration_parameters__table_into_df')
    q = """
          SELECT * from claims_analyst_parameters
          ORDER BY analyst
        """
    df = psql.read_sql(q, db['conn'])  
    return df

  

####################################
#
#   extending claims_extended with the duration columns
#
####################################


def drop__claims_with_durations__table(db):
    print('\nEntered drop__claims_with_durations__table')
    q = " DROP TABLE claims_with_durations"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_with_durations" ')
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

def create__claims_with_durations__table(db):
    print('\nEntered create__claims_extended_durations__table')
    q = """
          CREATE TABLE claims_with_durations as
          SELECT *
          FROM claims_extended
          WHERE Claim_num < 0;
          
          ALTER TABLE claims_with_durations
            ADD CONSTRAINT Claim_num_for_durations_as_KEY 
              PRIMARY KEY (Claim_num);
          
          ALTER TABLE claims_with_durations
          ADD_COLUMN analyst_duration_touch_compliance float8,
          ADD COLUMN payout_start_date date,
          ADD COLUMN return_to_work_date date,
          ADD COLUMN paid_leave_days int4,
          ADD COLUMN min_leave_days int4,
          ADD COLUMN recommended_touch_count int4,
          ADD COLUMN actual_touch_count int4
          -- need to add columns for actual touch dates
        """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created the table "claims_with_durations" ')
    # except:
    #     # if the attempt to drop table failed, then you have to rollback that request
    #     db['conn'].rollback()
    #     print('Table "claims_extended" did not exist')
    except Exception as e:
            db['conn'].rollback()
            print('  Failed to create table claims_with_durations')
            # """
            # to use this part, also adjust the "except" line 3 lines above
            print('  The exception error message is as follows:')
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
            # """

def pull__claims_extended__table_into_df(db): 
    print('\nEntering pull__claims_extended__table_into_df')
    q = """
          SELECT * from claims_extended
          ORDER BY Claim_Num
        """
    df = psql.read_sql(q, db['conn'])  
    return df

def compute_analyst_duration_touch_compliance(row):
    if row.decision != 'Approved':
        return 
    else:
        return 


def compute_payout_start_date(row):
    if row.decision != 'Approved':
        return
    else:
        return utils_general.biz_days_offset(row.date_received, 5)
    
def compute_paid_leave_days(row):
    if row.decision != 'Approved':
        return
    else:
        return    
    
def compute_paid_leave_days(row):
    if row.decision != 'Approved':
        return
    else:
        return

def compute_return_to_work_date(row):
    pass

def build__claims_with_durations__df(df):
    print('\nHave entered function to add the duration columns to the dataframe.')

    df['payout_start_date'] = df.apply(lambda row: compute_payout_start_date(row),
                                       axis=1)
    df['analyst_duration_touch_compliance'] = df.apply(lambda row: compute_analyst_duration_touch_compliance(row),
                                      axis=1)
    df['paid_leave_days'] = df.apply(lambda row: compute_paid_leave_days(row),
                                      axis=1)

    return df

    
def populate__claims_with_durations__table(df_dur, db):
    print('\nNeed to code function populate__claims_with_durations__table')
    





####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':

    start_datetime = datetime.now()
    print('\nThis program starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))

    dfs = {}
    
    # open postgres connection with mimic database
    db = utils_postgres.connect_postgres()


    # drop__diagnosis_duration_parameters__table(db)
    # create__diagnosis_duration_parameters__table(db)
    # import__diagnosis_duration_parameters_csv__into_postgres(db)
    df_dur = pull__diagnosis_duration_parameters__table_into_df(db)
    dfs['dur'] = df_dur
    # utils_general.display_df(df_dur)
    
    
    # drop__claims_analyst_parameters__table(db)
    # create__claims_analyst_parameters__table(db)
    # import__claims_analyst_parameters_csv__into_postgres(db)
    df_anal = pull__claims_analyst_parameters__table_into_df(db)
    dfs['anal'] = df_anal
    # utils_general.display_df(df_anal)
    
    drop__claims_with_durations__table(db)
    create__claims_with_durations__table(db)
    
    df_claims = pull__claims_extended__table_into_df(db)
    # utils_general.display_df(df_claims)
    
    df_dur = build__claims_with_durations__df(df_claims)
    utils_general.display_df(df_dur)
     
    populate__claims_with_durations__table(df_dur, db)

    print('\nWriting dataframe for claims_with_durations into csv file')
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    prefix = OPUS_DATA_OUTPUTS_DIR + timestamp + '__'
    df_dur.to_csv(prefix + 'claims_with_durations.csv', index=False)

    
    # close connection to the opus database    
    utils_postgres.close_postgres(db)
 
    
    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = utils_general.truncate(duration_in_s, 2)
    # minutes = utils_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    # hours = utils_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    print('\nThe script to augment claims_extended with durations data started running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('The duration in seconds was ' + str(seconds))
    # print('The duration in minutes was ' + str(minutes))
    # print('The duration in hours was  ' + str(hours))

    
    