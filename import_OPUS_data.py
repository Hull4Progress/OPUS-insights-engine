#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 18:03:15 2019

@author: rick
"""

from constants_used_for_insights_engine import *
import utils_general

import utils_general
import utils_postgres

from datetime import datetime


import sys

import pandas as pd
import numpy as np
# see pandas 1.0.3 api reference guid:
#   https://pandas.pydata.org/pandas-docs/stable/reference/index.html
#   older pandas: see https://pandas.pydata.org/pandas-docs/version/0.13/api.html, search for sql
import pandas.io.sql as psql


# see http://initd.org/psycopg/docs/usage.html
import psycopg2
# from psycopg2.extras import RealDictCursor
# see https://stackoverflow.com/questions/10252247/how-do-i-get-a-list-of-column-names-from-a-psycopg2-cursor
#  about half way down
import psycopg2.extras




####################################
#
#   Defining CLAIMS table and importing Ramesh sample data into it
#
####################################

def import_claims_csv_into_df():
    print('\nEntering function to import 5K claims csv into a dataframe')
    df = pd.read_csv(RAMESH_CLAIMS_CSV_5K)
    # print(list(df.columns.values))
    print('Successfully imported the 5K claims csv file')
    return df
    
def clean_df_column_names(df):
    print('\nEntering function to clean columns names of the df')
    col_name_list = list(df.columns.values)
    rename = {}
    for name in col_name_list:
        rename[name] = name.replace(' ', '_').replace('#', 'num').replace('/', '__').replace('>','gt')
        rename[name] = rename[name].replace('-','_').replace('_$', 'dollar').replace('$', 'dollar')
        rename[name] = rename[name].replace('srevice_','service')
        
        if rename[name][0] == '_':
            rename[name] = rename[name][1:]
        if rename[name][-1] == "'":
            rename[name] = rename[name][:-1]
        if rename[name][-1] == '_':
            rename[name] = rename[name][:-1]
        if rename[name][-1] == '_':
            rename[name] = rename[name][:-1]
        if rename[name][-1] == '_':
            rename[name] = rename[name][:-1]
        rename['Claims Analysts'] = 'Claims_Analyst'
        rename['IGO, NIGO'] = 'IGO_NIGO'
        rename['Follow up '] = 'NIGO_follow_up_hours'
        rename[' Preliminary analysis for decision plus nurse/no nurse'] = 'Prelim_dec_plus_nurse_no_nurse_hours'
        rename['Nurse review'] = 'Nurse_review_hours'
        rename['Reconsidered decision only for nurse review cases'] = 'Final_dec_if_nurse_review_hours'
        rename[' $ Cost per hour '] = 'Dollar_cost_per_hour'
        rename[' $ Cost per claim  '] = 'Dollar_cost_per_claim'
        
               
    print(str(rename))
    print()
  
    df.rename(columns = rename, inplace=True)
    
    return df
    
def drop_columns_that_will_be_computed_with_python(df):
    df = df.drop(['Unnamed:_8',
             'Total',                   # will compute Total hours
             'num_of_days_to_follow_up',
             'num_of_days_to_receive_information',
             'num_of_days_to_make__make_decision___ask_for_nurse_review',
             'num_of_days_for_nurse_to_make_the_decision',
             'num_of_days_to_take_decision_after_nurse_review',
             'Total_num_of_days_to_make_the_decision_from_date_of_receipt',
             'TAT_gt_7_days',
             'TAT_gt_14_days',
             'Inventory__Waiting_to_be_followed_up',
             'Inventory___Waiting_for_information_from_client',
             'Inventory___waiting_for_decision__ask_for_nurse_review',
             'Inventory___waiting_for_decision_after_nurse_review',
             'Claims_processed',
             'Dollar_cost_per_claim',
             'Claims_to_be_processed',
             'Penalty_for_claims_with_TAT_gt_7_days',
             'Penalty_for_claims_with_TAT_gt_14_days',
             'Penalty_for_claims_with_wrongly_declined',
             'Loss_Ratio',
             'Impact_on_customer_service_for_claims_with_TAT_gt_7_days',
             'Impact_on_customer_service__for_claims_with_TAT_gt_14_days',
             'Impact_on_customer_service__for_claims_with_wrongly_declined'], axis=1)
    
    print(list(df.columns.values))
    
    return df

def replace_nulls_with_values(df):
    df['NIGO_follow_up_hours'] = df['NIGO_follow_up_hours'].replace(np.nan, 0)
    df['Final_dec_if_nurse_review_hours'] = df['Final_dec_if_nurse_review_hours'].replace(np.nan, 0)
    df['Accurate_decision'] = df['Accurate_decision'].replace(np.nan, 0)
    df['Wrongly_approved'] = df['Wrongly_approved'].replace(np.nan, 0)
    df['Wrongly_declined'] = df['Wrongly_declined'].replace(np.nan, 0)
    df['Mode_of_follow_up'] = df['Mode_of_follow_up'].replace(np.nan, 'N/A')
    df['Dollar_cost_per_hour'] = df['Dollar_cost_per_hour'].replace(' $42 ', '42')
    return df


def display_df(df):
    # see https://thispointer.com/python-pandas-how-to-display-full-dataframe-i-e-print-all-rows-columns-without-truncation/
    #   to understand these and other display options
    # see also https://stackoverflow.com/questions/57860775/pandas-pd-options-display-max-rows-not-working-as-expected
    pd.set_option('display.max_columns', None)
    pd.set_option('display.min_rows', 30)
    print(df)
        

def drop__claims_raw__table(db):
    print('\nEntered drop__claims_raw__table')
    q = " DROP TABLE claims_raw "
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_raw" ')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Failed to drop table "claims_raw", perhaps because it did not exist')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """
		
		
def create__claims_raw__table(db):
    print('\nEntered create__claims_raw__table')
    q = """
          CREATE TABLE claims_raw (
             Claim_num int4 CONSTRAINT Claim_num_for_raw_as_KEY PRIMARY KEY,
		     Customer_num int4,
		     Salary_per_month int4,
		     Employee_level varchar,
		     Geo varchar,
		     Industry varchar,
		     Diagnosis varchar,
		     Claims_Analyst varchar,
		     NIGO_follow_up_hours float8,
		     Prelim_dec_plus_nurse_no_nurse_hours float8,
		     Final_dec_if_nurse_review_hours float8,
		     Nurse_review_hours float8,
		     Date_Received date,
		     IGO_NIGO varchar,
		     Mode_of_follow_up varchar,
		     Date_Follow_up_made date,
		     Date_all_information_received date,
		     Date_of_decision__ask_for_nurse_review date,
		     Is_nurse_review_required varchar,
		     Date_Nurse_Decision_Made date,
		     Date_of_decision_after_nurse_review date,
		     Decision varchar,
		     Accuracy_of_Decision varchar,
		     Accurate_decision int,
		     Wrongly_approved int,
		     Wrongly_declined int,
		     Dollar_cost_per_hour float
          )
	     """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created table "claims_raw", including a commit')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Attempt to create table "claims_raw" has failed')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """


def convert_dates_to_biz_days(df):
    print('\nNow entering function that will convert claims_raw from calendar days to business days')
    for col in ['Date_Received', 
                'Date_Follow_up_made', 
                'Date_all_information_received', 
                'Date_of_decision__ask_for_nurse_review', 
                'Date_Nurse_Decision_Made', 
                'Date_of_decision_after_nurse_review']:
        df[col] = df.apply(lambda row:  \
                     utils_general.convert_date2_after_date1_to_biz_date_after_date1('2019-11-01',
                                                                                     getattr(row, col)),
                     axis=1)
    return df

def drop__claims_raw_biz__table(db):
    print('\nEntered drop__claims_raw_biz__table')
    q = " DROP TABLE claims_raw_biz "
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_raw_biz" ')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Failed to drop table "claims_raw_biz", perhaps because it did not exist')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """
		
		
def create__claims_raw_biz__table(db):
    print('\nEntered create__claims_raw_biz__table')
    q = """
          CREATE TABLE claims_raw_biz (
             Claim_num int4 CONSTRAINT Claim_num_for_raw__biz_as_KEY PRIMARY KEY,
		     Customer_num int4,
		     Salary_per_month int4,
		     Employee_level varchar,
		     Geo varchar,
		     Industry varchar,
		     Diagnosis varchar,
		     Claims_Analyst varchar,
		     NIGO_follow_up_hours float8,
		     Prelim_dec_plus_nurse_no_nurse_hours float8,
		     Final_dec_if_nurse_review_hours float8,
		     Nurse_review_hours float8,
		     Date_Received date,
		     IGO_NIGO varchar,
		     Mode_of_follow_up varchar,
		     Date_Follow_up_made date,
		     Date_all_information_received date,
		     Date_of_decision__ask_for_nurse_review date,
		     Is_nurse_review_required varchar,
		     Date_Nurse_Decision_Made date,
		     Date_of_decision_after_nurse_review date,
		     Decision varchar,
		     Accuracy_of_Decision varchar,
		     Accurate_decision int,
		     Wrongly_approved int,
		     Wrongly_declined int,
		     Dollar_cost_per_hour float
          )
	     """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created table "claims_raw_biz", including a commit')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Attempt to create table "claims_raw_biz" has failed')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """





'''
 
def import_csv_into__claims_raw__table(db):
    print('\nEntered import_csv_into__claims_raw__table')

    q = """
         copy claims_raw (
	        Claim_Control_No,
	        Claim_Value_Dollars,
	        Geo,
	        Industry,
	        Diagnosis,
	        Claims_Analyst,
	        Claims_per_hour_per_analyst,
	        Date_Received,
	        IGO_NIGO,
            Blank1,
	        Date_Follow_up_made,
            Blank2,
	        Date_all_information_received,
	        Is_nurse_review_required,
            Blank3,
	        Date_Nurse_Decision_Made,
            Blank4,
	        Date_Final_Decision_Made,
	        Decision,
            Blank5,
            Blank6,
	        Adherence_to_Performance_Guarantee,
            Accuracy_of_Decision
          )
          from stdin with
          csv
          header
          delimiter as ','
        """
    
    fileName = RAMESH_CLAIMS_CSV
    
    f = open(fileName, 'r')
    db['cursor'].copy_expert(sql=q, file=f)
    f.close()
    db['conn'].commit()

def drop_columns_from__claims_raw__table(db):
    print('\nEntered drop_columns_from__claims__table')
    q = """
          ALTER TABLE claims_raw
          DROP COLUMN IF EXISTS Blank1,
          DROP COLUMN IF EXISTS Blank2,
          DROP COLUMN IF EXISTS Blank3,
          DROP COLUMN IF EXISTS Blank4,
          DROP COLUMN IF EXISTS Blank5,
          DROP COLUMN IF EXISTS Blank6
          
        """
    db['cursor'].execute(q)
    db['conn'].commit()
    print('Dropped blank columns from table "claims_raw", including a commit')

'''


####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    
    start_datetime = datetime.now()
    print('\nThis program starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    
    
    df = import_claims_csv_into_df()
    df = clean_df_column_names(df)
    df = drop_columns_that_will_be_computed_with_python(df)
    df = replace_nulls_with_values(df)
    print()
    display_df(df)
    
    
    # open postgres connection with mimic database
    db = utils_postgres.connect_postgres()
        
    drop__claims_raw__table(db)
    create__claims_raw__table(db)
    utils_postgres.load_df_into_table_with_same_columns(df, db, 'claims_raw')
    
    # display_df(df)
      
    df_biz = convert_dates_to_biz_days(df)
    
    # display_df(df_biz)
    
    drop__claims_raw_biz__table(db)
    create__claims_raw_biz__table(db)
    utils_postgres.load_df_into_table_with_same_columns(df_biz, db, 'claims_raw_biz')

    # close connection to the mimic database    
    utils_postgres.close_postgres(db)
    
    
    print('\nNow writing various dataframes to csv files')
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    prefix = OPUS_DATA_OUTPUTS_DIR + timestamp + '__'
    df.to_csv(prefix + 'claims_raw.csv', index=False)
    df_biz.to_csv(prefix + 'claims_raw_biz.csv', index=False)
    
    
    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = utils_general.truncate(duration_in_s, 2)
    minutes = utils_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    hours = utils_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    print('\nThe import_OPUS_data script started running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('The duration in seconds was ' + str(seconds))
    print('The duration in minutes was ' + str(minutes))
    print('The duration in hours was  ' + str(hours))
       
    
