#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 18:50:22 2020

@author: rick
"""

import sys

sys.path.append('../Constants')
from constants_used_for_insights_engine import *

sys.path.append('../Utilities')
import utils_general
import utils_postgres


from datetime import datetime

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

import random



##########################################
#
#   Pulling in RAW biz table
#
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


'''
def compute_hours_worked(nigo_following_up_hours,
                         decidong_1_hours,
                         nurse_reviewing_hours,
                         deciding_2_hours):
    return nigo_following_up_hours + \
                         deciding_1_hours + \
                         nurse_reviewing_hours + \
                         deciding_2_hours
'''

##########################################
#
#   adding INTAKE columns
#
##########################################


def build_intake_analyst_dict():
    dict = {}
    dict[0] = 'Anika Agarwal'
    dict[1] = 'Kiara Ahuja'
    dict[2] = 'Prisha Muthu'
    dict[3] = 'Aditya Patel'
    dict[4] = 'Umesh Anand'
    dict[5] = 'Akhil Anand'
    return dict    

def assign_intake_analyst_to_claim(row):
    rand = random.randrange(6)
    # print ('random number is: '  + str(rand))
    return rand

def compute_intake_deciding_hours(row):
    rand = random.gauss(.5, .1)
    rand = round(max(.2, min(.8, rand)), 2)
    # print ('random intake deciding time is: ' + str(rand))
    return rand
    
def compute_received_to_intake_decided_biz_days(row):
    if row.intake_decided_date.day <= 11:
        midpoint = 1.5
    else:
        midpoint = 2.5
    rand = random.gauss(midpoint,0.7)
    rand = round(max(0, min(5, rand)))
    # print('random biz days for intake decisioning is: ' + str(rand))
    return rand
    
def compute_received_date(row):
    received_date = utils_general.biz_days_offset(row.intake_decided_date, -1 * row.received_to_intake_decided_biz_days)
    return received_date
    

'''
This function is adding a new stage to the processing in Ramesh's synthetic claims database
   In particualr we are adding an "intake_deciding" stage, that ends with "intake_decided"
   We have added a new kind of analyst -- intake_analyst -- these are based in India
   The decision will take under an hour of work
   The decision will take between 0 and 4 days, with longer decisions happening in Dec and Feb (and maybe April)
      (and maybe April; we are creating this for the 5K claims, and then replicating for the following months)
'''
def build__claims_with_intake__df(df):
    # rename 'received_date' column to 'intake_decided_date'
    df.rename(columns = {'received_date' : 'intake_decided_date'}, inplace=True)
    
    intake_anal_dict = build_intake_analyst_dict()
    
    df['intake_analyst'] = df.apply(lambda row: intake_anal_dict[assign_intake_analyst_to_claim(row)],
                                          axis=1)

    df['intake_deciding_hours'] = df.apply(lambda row: compute_intake_deciding_hours(row),
                                               axis=1)

    df['received_to_intake_decided_biz_days'] = df.apply(lambda row: compute_received_to_intake_decided_biz_days(row),
                                                    axis=1)
    
    df['received_date'] =  df.apply(lambda row: compute_received_date(row),
                                               axis=1)
    
    # utils_general.display_df(df)
    return df



##########################################
#
#   adding columns about TAT and days duration 
#
##########################################



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

def build__claims_extended__df(df):  
    print('\nHave entered function to add the derived columns to the dataframe.')

    df['total_claims_analyst_hours'] = df.apply(lambda row: row.nigo_following_up_hours +\
                                                            row.deciding_1_hours +\
                                                            row.deciding_2_hours,
                                          axis=1)
    
    df['total_hours'] = df.apply(lambda row: row.intake_deciding_hours +\
                                             row.nigo_following_up_hours +\
                                             row.deciding_1_hours +\
                                             row.nurse_reviewing_hours +\
                                             row.deciding_2_hours,
                                          axis=1)
    
    df['total_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.received_date,
                                                                         row.decided_2_date),
                                    axis=1)
    
    df['over_five_biz_days'] = df.apply(lambda row: compute_above_5_biz_days(row.total_biz_days),
                                    axis=1)
    
    df['over_ten_biz_days'] = df.apply(lambda row: compute_above_10_biz_days(row.total_biz_days),
                                    axis=1)

    df['received_to_intake_decided_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.received_date,
                                                                         row.intake_decided_date),
                                    axis=1)    

    df['intake_decided_to_nigo_followed_up_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.intake_decided_date,
                                                                         row.nigo_followed_up_date),
                                    axis=1)    

    df['nigo_followed_up_to_all_info_received_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.nigo_followed_up_date,
                                                                         row.all_info_received_date),
                                    axis=1)    

    # note: if the claim is IGO, then intake_decided_date = nigo_follows_up_date = all_info_received_date
    df['all_info_received_to_decided_1_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.all_info_received_date,
                                                                         row.decided_1_date),
                                    axis=1)    

    df['decided_1_to_nurse_reviewed_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.decided_1_date,
                                                                         row.nurse_reviewed_date),
                                    axis=1)    

    df['nurse_reviewed_to_decided_2_biz_days'] = df.apply(lambda row: compute_biz_days_between(row.nurse_reviewed_date,
                                                                         row.decided_2_date),
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
    print('\nEntered create__claims_extended__table')
    # This new table has many columns that are also in claims_raw_biz.  In principle I should
    #   be clever and avoid re-writing all of the column names from claims_raw_biz. 
    #   For example, I could define a query string that has some parameters inside -- But I
    #     am not doing that, since the creation of that table and the creation of the table here
    #     is happening in separate python files. (
    #   Or, I could define a new Postgres table based on a copy of the claims_raw_biz table 
    #     (but not include any of the records) and then add new columns.  But I'm not doing
    #     that because Postgres doesn't support changing column order in tables
    q = """
          CREATE TABLE claims_extended (
             claim_num int4 CONSTRAINT Claim_num_for_extended_as_KEY PRIMARY KEY,
		     customer_num int4,
		     salary_per_month int4,
		     employee_level varchar,
		     geo varchar,
		     industry varchar,
		     diagnosis varchar,
             intake_analyst varchar,           -- this is new for claims_extended
		     claims_analyst varchar,
		     igo_nigo varchar,
		     nigo_followed_up_mode varchar,
		     is_nurse_review_required varchar,
             intake_deciding_hours float8,     -- this is new for claims_extended
		     nigo_following_up_hours float8,
		     deciding_1_hours float8,
		     nurse_reviewing_hours float8,
		     deciding_2_hours float8,
		     received_date date,
             intake_decided_date date,         -- this is new for claims_extended
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
		     dollar_cost_per_hour float,
             total_claims_analyst_hours float8,      -- here and below are new for claims_extended
             total_hours float8,
             total_biz_days int4,
             over_five_biz_days int4,
             over_ten_biz_days int4,
             received_to_intake_decided_biz_days int4,
             intake_decided_to_nigo_followed_up_biz_days int4,
             nigo_followed_up_to_all_info_received_biz_days int4,
             all_info_received_to_decided_1_biz_days int4,
             decided_1_to_nurse_reviewed_biz_days int4,
             nurse_reviewed_to_decided_2_biz_days int4
          )
         """

    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created the table "claims_extended" ')
    except Exception as e:
            db['conn'].rollback()
            print('  Failed to create table claims_extended' )
            print('  The exception error message is as follows:')
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)

def push_df_into__claims_extended__table(df, db):
    print('\nHave entered function push_df_into__claims_extended__table')

    table_name = 'claims_extended'
    utils_postgres.load_df_into_table_with_same_columns(df, db, table_name)
    

'''
# Not using this anymore, because all of the group-by aggregates will be focused on date intervals

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
    db = utils_postgres.connect_postgres()

    df_raw = pull__claims_raw_biz__table_into_df(db)   # .head(50)
    
    df_intake = build__claims_with_intake__df(df_raw)
    
    df_ext = build__claims_extended__df(df_intake)
    print(df_ext)
    utils_general.display_df(df_ext)
    print(list(df_ext.columns.values))
    
    drop__claims_extended__table(db)
    create__claims_extended__table(db)
    
    # using psycopg2 and psycopg2.extras:
    utils_postgres.load_df_into_table_with_same_columns(df_ext, db, 'claims_extended')
    
    print('\nWriting dataframe for claims_extended into csv file')
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    prefix = OPUS_DATA_OUTPUTS_DIR + timestamp + '__'
    df_ext.to_csv(prefix + 'claims_extended.csv', index=False)
    
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
    # print('The duration in minutes was ' + str(minutes))
    # print('The duration in hours was  ' + str(hours))
       

    
