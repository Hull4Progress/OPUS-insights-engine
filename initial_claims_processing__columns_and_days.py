#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 18:50:22 2020

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

##########################################

def pull__claims_raw_biz__table_into_df(db):
    print('\nEntering pull__claims_raw_biz__table_into_df')
    q = """
          SELECT * from claims_raw_biz
          ORDER BY claim_num
        """
    df = psql.read_sql(q, db['conn'])  
    return df

'''
# this one extrapolates an hours worked based on claims_per_hour of analyst, 
#      and claim_value_dollars.
#      the parameter claim_control_no is used to enable convenient printing
#          of the hours_worked value for the first 20 rows 
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
'''

def compute_hours_worked(nigo_follow_up_hours,
                         prelim_dec_plus_nurse_no_nurse_hours,
                         nurse_review_hours,
                         final_dec_if_nurse_review_hours):
    return nigo_follow_up_hours + \
                         prelim_dec_plus_nurse_no_nurse_hours + \
                         nurse_review_hours + \
                         final_dec_if_nurse_review_hours

def compute_biz_days_between(date1, date2):
    return utils_general.biz_days_between_dates(date1, date2)

def compute_above_5_biz_days(total_biz_days):
    # duration = utils_general.biz_days_between_dates(date1, date2)
    if total_biz_days <= 5:
        return 0
    else:
        return 1

def compute_above_10_biz_days(total_biz_days):
    # duration = utils_general.biz_days_between_dates(date1, date2)
    if total_biz_days <= 10:
        return 0
    else:
        return 1

def add_columns_to_df(df):  
    print('\nHave entered function to add the derived columns to the dataframe.')

    df['total_hours'] = df.apply(lambda row: compute_hours_worked(row.nigo_follow_up_hours,
                                                       row.prelim_dec_plus_nurse_no_nurse_hours,
                                                       row.nurse_review_hours,
                                                       row.final_dec_if_nurse_review_hours),
                                          axis=1)
    
    df['total_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_received,
                                                                         row.date_of_decision_after_nurse_review),
                                    axis=1)
    
    df['over_five_biz_days'] = df.apply(lambda row: compute_above_5_biz_days(row.total_biz_days),
                                    axis=1)
    
    df['over_ten_biz_days'] = df.apply(lambda row: compute_above_10_biz_days(row.total_biz_days),
                                    axis=1)

    df['nigo_follow_up_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_received,
                                                                         row.date_follow_up_made),
                                    axis=1)    

    df['nigo_requested_to_all_info_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_follow_up_made,
                                                                         row.date_all_information_received),
                                    axis=1)    

    df['all_info_received_to_prelim_dec_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_all_information_received,
                                                                         row.date_of_decision__ask_for_nurse_review),
                                    axis=1)    

    df['nurse_dec_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_of_decision__ask_for_nurse_review,
                                                                         row.date_nurse_decision_made),
                                    axis=1)    

    df['nurse_dec_to_final_dec_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.date_nurse_decision_made,
                                                                         row.date_of_decision_after_nurse_review),
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
          FROM claims_raw_biz
          WHERE Claim_num < 0;
          
          -- ALTER TABLE claims_extended
          --   DROP CONSTRAINT Claim_num_for_raw_as_KEY;
            
          ALTER TABLE claims_extended
            ADD CONSTRAINT Claim_num_for_extended_as_KEY 
              PRIMARY KEY (Claim_num);
          
          ALTER TABLE claims_extended
          ADD COLUMN total_hours float8,
          ADD COLUMN total_biz_days int4,
          ADD COLUMN over_five_biz_days int4,
          ADD COLUMN over_ten_biz_days int4,
          ADD COLUMN nigo_follow_up_biz_days int4,
          ADD COLUMN nigo_requested_to_all_info_biz_days int4,
          ADD COLUMN all_info_received_to_prelim_dec_biz_days int4,
          ADD COLUMN nurse_dec_biz_days int4,
          ADD COLUMN nurse_dec_to_final_dec_biz_days int4
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
            print('  Failed to create table claims_extended' )
            # """
            # to use this part, also adjust the "except" line 3 lines above
            print('  The exception error message is as follows:')
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
            # """

def push_df_into__claims_extended__table(df, db):
    print('\nHave entered function push_df_into__claims_extended__table')

    table_name = 'claims_extended'
    utils_postgres.load_df_into_table_with_same_columns(table_name, df, db)
    


####################################
#
#   Building BASIC claims_agg table
#
####################################

def drop__claims_agg__table(db):
    print('\nEntered drop__claims_agg__table')
    q = " DROP TABLE claims_agg"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_agg" ')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table "claims_agg" did not exist')


# following the CTAS comment in 
#    https://stackoverflow.com/questions/43453120/creating-new-table-from-select-statement
# also, following https://stackoverflow.com/questions/13113096/how-to-round-an-average-to-2-decimal-places-in-postgresql/20934099
#    with regards to casting the AVG floats to be NUMERIC so that I can apply ROUND in postgres
def build_basic__claims_agg__table(db):
    print('\nEntered function build_basic__claims_agg__table')
    q = """
          CREATE TABLE claims_agg AS
          SELECT 
            diagnosis, industry, geo, 
            claims_analyst, igo_nigo,
            is_nurse_review_required, adherence_to_performance_guarantee,
            accuracy_of_decision,
            --
            count(*) as total_count,
            SUM(over_five_biz_days) as total_over_five_biz_days,
            SUM(over_ten_biz_days) as total_over_ten_biz_days,           
            ROUND(AVG(claim_value_dollars)::numeric, 2) as avg_claim_value_dollars,
            ROUND(AVG(hours_for_this_claim)::numeric, 2) as avg_hours_per_claim,
            ROUND(AVG(nigo_follow_up_biz_days)::numeric, 2) as avg_nigo_follow_up_biz_days,
            ROUND(AVG(nigo_requested_to_all_info_biz_days)::numeric, 2) as avg_nigo_requested_to_all_info_biz_days,
            ROUND(AVG(nigo_received_to_all_info_biz_days)::numeric, 2) as avg_nigo_received_to_all_info_biz_days,
            ROUND(AVG(nurse_decision_biz_days)::numeric, 2) as avg_nurse_decision_biz_days,
            ROUND(AVG(total_biz_days)::numeric, 2) as avg_total_biz_days
          FROM claims_extended
          GROUP BY
            diagnosis, industry, geo, 
            claims_analyst, igo_nigo,
            is_nurse_review_required, adherence_to_performance_guarantee,
            accuracy_of_decision
          ORDER BY
            diagnosis, industry, geo, 
            claims_analyst, igo_nigo,
            is_nurse_review_required, adherence_to_performance_guarantee,
            accuracy_of_decision
          
        """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created the table "claims_agg" ')
    except Exception as e:
            db['conn'].rollback()
            print('  Failed to create table "claims_agg" ')
            # """
            # to use this part, also adjust the "except" line 3 lines above
            print('  The exception error message is as follows:')
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
            # """



        
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
    db = utils_postgres.connect_postgres()

    

    df = pull__claims_raw_biz__table_into_df(db)
    df = add_columns_to_df(df)
    print(df)
    print(list(df.columns.values))
    
    drop__claims_extended__table(db)
    create__claims_extended__table(db)
    
    # using psycopg2 and psycopg2.extras:
    utils_postgres.load_df_into_table_with_same_columns(df, db, 'claims_extended')
    
    print('\nWriting dataframe for claims_extended into csv file')
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    prefix = OPUS_DATA_OUTPUTS_DIR + timestamp + '__'
    df.to_csv(prefix + 'claims_extended.csv', index=False)
    

    '''
    drop__claims_agg__table(db)
    build_basic__claims_agg__table(db)
    '''    


    # close connection to the mimic database    
    utils_postgres.close_postgres(db)

    
    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = utils_general.truncate(duration_in_s, 2)
    minutes = utils_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    hours = utils_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    print('\nThe initial_claims_processing script starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('The duration in seconds was ' + str(seconds))
    print('The duration in minutes was ' + str(minutes))
    print('The duration in hours was  ' + str(hours))
       

    
