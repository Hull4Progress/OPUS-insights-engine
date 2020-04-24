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

    # making all names lower case
    rename = {}
    col_name_list = list(df.columns.values)
    for name in col_name_list:
        rename[name] = name.lower()
    df.rename(columns = rename, inplace=True)


    # cleaning names, e.g., removing blanks, '/', '#', '$', etc
    rename = {}
    col_name_list = list(df.columns.values)    
    for name in col_name_list:
        rename[name] = name.replace(' ', '_').replace('#', 'num').replace('/', '__').replace('>','gt').\
                          replace('-','_').replace('_$', 'dollar').replace('$', 'dollar').\
                          replace('srevice_','service')
        
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
        
    print('The name replacement focused on first round of cleaning up the original names is as follows:\n')
    print(str(rename))
    print()
    df.rename(columns = rename, inplace=True)        
    
    print('\nAfter cleaning column names, the column list is:\n')
    print(list(df.columns.values))

    # replacing some names with more succinct names
    rename = {}
    rename['date_received'] = 'received_date'
    rename['claims_analysts'] = 'claims_analyst'
    rename['igo,_nigo'] = 'igo_nigo'
    rename['follow_up'] = 'nigo_following_up_hours'
    rename['preliminary_analysis_for_decision_plus_nurse__no_nurse'] = 'deciding_1_hours'
    rename['nurse_review'] = 'nurse_reviewing_hours'
    rename['reconsidered_decision_only_for_nurse_review_cases'] = 'deciding_2_hours'       
    rename['nurse_review_hours'] = 'nurse_reviewing_hours'
    rename['mode_of_follow_up'] = 'nigo_followed_up_mode'
    rename['date_follow_up_made'] = 'nigo_followed_up_date'
    rename['date_all_information_received'] = 'all_info_received_date'
    rename['date_of_decision__ask_for_nurse_review'] = 'decided_1_date'
    rename['date_nurse_decision_made'] = 'nurse_reviewed_date'
    rename['date_of_decision_after_nurse_review'] = 'decided_2_date'


    print('The name replacement focused on replacing original names with more succinct names is as follows:\n')
    print(str(rename))
    print()

    df.rename(columns = rename, inplace=True)
    
    print('\nAfter cleaning column names and replacing some of them, the column list is:')
    print(list(df.columns.values))

    
    return df
    
def drop_columns_that_will_be_computed_with_python(df):
    df = df.drop(['unnamed:_8',
             'total',                   # will compute Total hours
             'num_of_days_to_follow_up',
             'num_of_days_to_receive_information',
             'num_of_days_to_make__make_decision___ask_for_nurse_review',
             'num_of_days_for_nurse_to_make_the_decision',
             'num_of_days_to_take_decision_after_nurse_review',
             'total_num_of_days_to_make_the_decision_from_date_of_receipt',
             'tat_gt_7_days',
             'tat_gt_14_days',
             'inventory__waiting_to_be_followed_up',
             'inventory___waiting_for_information_from_client',
             'inventory___waiting_for_decision__ask_for_nurse_review',
             'inventory___waiting_for_decision_after_nurse_review',
             'claims_processed',
             'dollar_cost_per_claim',
             'claims_to_be_processed',
             'penalty_for_claims_with_tat_gt_7_days',
             'penalty_for_claims_with_tat_gt_14_days',
             'penalty_for_claims_with_wrongly_declined',
             'loss_ratio',
             'impact_on_customer_service_for_claims_with_tat_gt_7_days',
             'impact_on_customer_service__for_claims_with_tat_gt_14_days',
             'impact_on_customer_service__for_claims_with_wrongly_declined'], axis=1)
    
    print('\nAfter dropping columns whose values will be computed with python the column list is:')
    print(list(df.columns.values))
    
    return df

def replace_nulls_with_values(df):
    df['nigo_following_up_hours'] = df['nigo_following_up_hours'].replace(np.nan, 0)
    df['deciding_1_hours'] = df['deciding_1_hours'].replace(np.nan, 0)
    df['deciding_2_hours'] = df['deciding_2_hours'].replace(np.nan, 0)
    df['accurate_decision'] = df['accurate_decision'].replace(np.nan, 0)
    df['wrongly_approved'] = df['wrongly_approved'].replace(np.nan, 0)
    df['wrongly_declined'] = df['wrongly_declined'].replace(np.nan, 0)
    df['nigo_followed_up_mode'] = df['nigo_followed_up_mode'].replace(np.nan, 'N/A')
    df['dollar_cost_per_hour'] = df['dollar_cost_per_hour'].replace(' $42 ', '42')
    return df


def display_df(df):
    # see https://thispointer.com/python-pandas-how-to-display-full-dataframe-i-e-print-all-rows-columns-without-truncation/
    #   to understand these and other display options
    # see also https://stackoverflow.com/questions/57860775/pandas-pd-options-display-max-rows-not-working-as-expected
    pd.set_option('display.max_columns', None)
    pd.set_option('display.min_rows', 30)
    print(df)
        



def convert_dates_to_biz_days(df):
    print('\nNow entering function that will convert claims_raw from calendar days to business days')
    for col in ['received_date', 
                'nigo_followed_up_date', 
                'all_info_received_date', 
                'decided_1_date', 
                'nurse_reviewed_date', 
                'decided_2_date']:
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
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
		
		
def create__claims_raw_biz__table(db):
    print('\nEntered create__claims_raw_biz__table')
    q = """
          CREATE TABLE claims_raw_biz (
             claim_num int4 CONSTRAINT Claim_num_for_raw_biz_as_KEY PRIMARY KEY,
		     customer_num int4,
		     salary_per_month int4,
		     employee_level varchar,
		     geo varchar,
		     industry varchar,
		     diagnosis varchar,
		     claims_analyst varchar,
		     igo_nigo varchar,
		     nigo_followed_up_mode varchar,
		     is_nurse_review_required varchar,
		     nigo_following_up_hours float8,
		     deciding_1_hours float8,
		     nurse_reviewing_hours float8,
		     deciding_2_hours float8,
		     received_date date,
		     nigo_followed_up_date date,
		     all_info_received_date date,
		     decided_1_date date,
		     nurse_reviewed_date date,
		     decided_2_date date,
		     decision varchar,
		     accuracy_of_decision varchar,
		     accurate_decision int,
		     wrongly_approved int,
		     wrongly_declined int,
		     dollar_cost_per_hour float
          )

        """

    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created table "claims_raw_biz", including a commit')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Attempt to create table "claims_raw_biz" has failed')
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)






####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    
    start_datetime = datetime.now()
    print('\nThis program starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    
    
    df_raw = import_claims_csv_into_df()
    df_raw = clean_df_column_names(df_raw)
    df_raw = drop_columns_that_will_be_computed_with_python(df_raw)
    df_raw = replace_nulls_with_values(df_raw)

    df_biz = convert_dates_to_biz_days(df_raw)

    print('\nHere is a display of the df_biz dataframe:\n')
    display_df(df_biz)
        
    # open postgres connection with mimic database
    db = utils_postgres.connect_postgres()
                  
    drop__claims_raw_biz__table(db)
    create__claims_raw_biz__table(db)
    utils_postgres.load_df_into_table_with_same_columns(df_biz, db, 'claims_raw_biz')

    # close connection to the mimic database    
    utils_postgres.close_postgres(db)
    
    
    print('\nNow writing various dataframes to csv files')
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    prefix = OPUS_DATA_OUTPUTS_DIR + timestamp + '__'
    # df_raw.to_csv(prefix + 'claims_raw.csv', index=False)
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
       
    
