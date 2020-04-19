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
             Percent_0_Touches int4,
             Touch_1_Target_Day int4,
             Touch_2_Target_Day int4,
             Touch_3_Target_Day int4,
             Touch_4_Target_Day int4,
             Touch_5_Target_Day int4
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
           Percent_0_Touches,
           Touch_1_Target_Day,
           Touch_2_Target_Day,
           Touch_3_Target_Day,
           Touch_4_Target_Day,
           Touch_5_Target_Day
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
#   creating dataframe that extends claims_extended with the duration columns
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

def compute_analyst_duration_touch_compliance(row, dfs):
        analyst = row.claims_analyst
        anal_row = dfs['anal'].loc[dfs['anal']['analyst'] == analyst]
        anal_dict = anal_row.to_dict('records')[0]
        compliance = anal_dict['duration_touch_compliance'] 
        return compliance

# this is based heavily on the representation/foramtting used for the
#    file Diagnosis_Duration_Parameters_v01.csv
# In particular, the columns percent_0_touchess, perceont_1_touches, etc
#    give a raw percentage for that number.  So to get the range
#    for, e.g., percent_2_touches we need to look for the randomaizer*100 being
#    between percent_9_touches + percent_1_touches <= randomizer*100 <=
#                 percent_0_touches + percent_1_touches + percent_2_touches
def map_rand_to_touch_count(r, dict, row):
    r = r*100
    if r <= dict['percent_0_touches']:
        return 0
    elif r <= dict['percent_1_touches'] + dict['percent_0_touches']:
        return 1
    elif r <= dict['percent_2_touches'] + dict['percent_1_touches'] + dict['percent_0_touches']:
        return 2
    elif r <= dict['percent_3_touches'] + \
                dict['percent_2_touches'] + dict['percent_1_touches'] + dict['percent_0_touches']:
        return 3
    elif r <= dict['percent_4_touches'] + dict['percent_3_touches'] + \
                dict['percent_2_touches'] + dict['percent_1_touches'] + dict['percent_0_touches']:
        return 4
    else:
        return 5


# the row.analyst_duration_touch_compliance gives a value for how much the analyst complies with
#    the recommended number of touches.  This is read in from Claims_Analyst_Parameters.csv,
#    and has values between 1.5 and 0.6.  
# We start with a random number in [0,1) and then weight it by multiplying with the touch_compliance number
# We use the map_rand_to_touch() to bring in the overall percentages for dufferent touch counts
#     from Diagnosis_Duration_Parameters.csv
def compute_actual_dur_touch_count(row, dfs):
    if row.decision != 'Approved':
        return 
    else:
        randomizer = min(.9999, random.random() * row.analyst_duration_touch_compliance)
        diagnosis = row.diagnosis
        dur_row = dfs['dur'].loc[dfs['dur']['diagnosis'] == diagnosis]
        dur_dict = dur_row.to_dict('records')[0]
        count = map_rand_to_touch_count(randomizer, dur_dict, row)
        # print(diagnosis + ', ' + str(randomizer) + ', '  + str(count))
        return count

def compute_recommended_dur_touch_count(row, dfs):
    if row.decision != 'Approved':
        return 
    else:
        diagnosis = row.diagnosis
        dur_row = dfs['dur'].loc[dfs['dur']['diagnosis'] == diagnosis]
        dur_dict = dur_row.to_dict('records')[0]
        return dur_dict['rec_touch_number']

def compute_dur_touch_actual_v_rec_percentage(row):
    if row.decision != 'Approved':
        return 
    elif row.recommended_dur_touch_count == 0:
        return 100
    else:
        return round(100 * (row.actual_dur_touch_count / row.recommended_dur_touch_count))
       


def compute_payout_start_date(row):
    if row.decision != 'Approved':
        return
    else:
        return utils_general.biz_days_offset(row.date_received, 5)
    
# We assign the number of paid leave days based on 
#    the range between min_duration_days and max_duration_days
#    weighted by (the recommended number of touches) - (the actual number of touches)
#       (so, no touches yields the max number of days)
# We also throw in a randomaizer, giving couple of days in either direction
def compute_actual_duration_days(row):
    if row.decision != 'Approved':
        return 
    else:
        count = 0
        diagnosis = row.diagnosis
        dur_row = dfs['dur'].loc[dfs['dur']['diagnosis'] == diagnosis]
        dur_dict = dur_row.to_dict('records')[0]
        if dur_dict['rec_touch_number'] == 0:
            count = dur_dict['min_duration_days']
        else:
            count = dur_dict['min_duration_days'] + \
                  ((dur_dict['max_duration_days'] - dur_dict['min_duration_days']) * \
                     ((dur_dict['rec_touch_number'] - row.actual_dur_touch_count) / dur_dict['rec_touch_number'] ))
        rand_count = count + round(random.gauss(0,count/20))
        # print (diagnosis, str(dur_dict['min_duration_days']), dur_dict['max_duration_days'], str(count), str(rand_count))
        return rand_count

def compute_min_duration_days(row, dfs):
    if row.decision != 'Approved':
        return 
    else:
        diagnosis = row.diagnosis
        dur_row = dfs['dur'].loc[dfs['dur']['diagnosis'] == diagnosis]
        dur_dict = dur_row.to_dict('records')[0]
        return dur_dict['min_duration_days']
    

# we assume here that the number 'paid_leave_days' is calendar days
# So se first compute the day which is exactly paid_leave_days after the 
#    payout_start_date. 
# (As a refinement we could adjust forward if that day is not a business day,
#    but actually, maybe some of the workers have to work weekends, etc)
def compute_return_to_work_date(row, dfs):
    if row.decision != 'Approved':
        return
    else:
        cal_date = utils_general.cal_days_offset(row.payout_start_date,
                                                 row.actual_duration_days)
        return cal_date
    
def compute_dur_touch_target_date(i, row, dfs):
    if row.decision != 'Approved':
        return
    else:
        diagnosis = row.diagnosis
        dur_row = dfs['dur'].loc[dfs['dur']['diagnosis'] == diagnosis]
        dur_dict = dur_row.to_dict('records')[0]
        field = 'touch_' + str(i) + '_target_day'
        count = dur_dict[field]
        if pd.isnull(count):
            # print('found a null count value')
            return
        else:
            target_cday = utils_general.cal_days_offset(row.payout_start_date,
                                                        count)
            target_bday = utils_general.biz_day_on_or_immed_after(target_cday)
            # print(row.payout_start_date, target_cday, target_bday)
            return target_bday
            
'''
We have to deal with the following combinations for rec_dur_touch_count and act_dur_touch_count
   5, 5: then the i-th actual touch point should be on or near the i-th rec touch point
   5, 4: we will skip the 2nd rec touch point
   5, 3: we will skip the 2nd and 4th touch points
   5, 2: we will skip 2nd, 4th and 5th touch points
   5, 1: we will hit 2nd touch point
       
   4, 4: do all
   4, 3: skip 2nd rec touch point
   4, 2: skip 2nd and 4th rec touch points
   4, 1: hit 2nd rec touch point
   
   3, 3: do all
   3, 2: skip 2nd rec touch point
   3, 1: hit 1st rec touch point
       
   2, 2: do all
   2, 1: hit 1st rec touch piont
       
   1, 1: do all

We use the following rules.  Note that for cases of 1 < actual < rec we basically skip the 2nd rec
    (This is a simplifying assumption but will still give enough of a random feel)
    
if we want the i-th actual date, but i > dur_actual_touch_count then we return empty
if a = r then do all
if a = 1 < r then do the middle one
if a = 2 < r then 
      actual 1 = rec 1 + random
      actual 2 = rec 3 + random
if a = 3 < r then
      actual 1 = rec 1 + random
      actual 2 = rec 3 + random
      actual 3 = rec 4 + random
if a = 4 < r then
      actual 1 = rec 1 + random
      actual 2 = rec 3 + random
      actual 3 = rec 4 + random
      actual 4 = rec 5 + random

'''
def final_date(row, target_count):
    # print('target_count is: ' + str(target_count))
    target_field = 'dur_touch_' + str(target_count) + '_target_date'
    target_date = getattr(row, target_field)
    # print('target_date is: ' + target_date)
    rand = random.randrange(5)
    # print('rand is: ' + str(rand))
    final_date = utils_general.biz_days_offset(target_date, rand)
    # print(str('final_date is: ' + str(final_date)))
    return final_date

def compute_dur_touch_actual_date(i, row, dfs):
    if row.decision != 'Approved':
        return
    else:
        diagnosis = row.diagnosis
        dur_row = dfs['dur'].loc[dfs['dur']['diagnosis'] == diagnosis]
        dur_dict = dur_row.to_dict('records')[0]
        # print('\ni has value: ' + str(i))
        # print('row.payout_start_date is: ' + row.payout_start_date)
        rec_count = int(row.recommended_dur_touch_count)
        # print('rec_count is: ' + str(int(rec_count)))
        act_count = int(row.actual_dur_touch_count)
        # print('act_count is: ' + str(act_count))
        if i > act_count:
            # print(str('i > act_count, i.e., ' + str(i) + ' > ' + str(act_count) + ', so, no-op'))
            return
        elif act_count == 1:       # put target date about in the middle of all the target dates
            # print('i = act_count = 1, so we will put act_touch_1 in the middle of rec_touches')
            target_count = math.floor(dur_dict['rec_touch_number']/2)
            return final_date(row, target_count)
        elif act_count == rec_count:
            # print('act_count = rec_count = ' + str(act_count) + ' so will set act_touch_i = rec_touch_i')
            target_count = i
            return final_date(row, target_count)
        else:     # if here then act_count != 1 and act_count != rec_count and i <= act_count
            # print('1 < act_count <= i and act_touch != rec_touch')
            if i == 1:
                target_count = 1
            else:
                target_count = i+1
            return final_date(row, target_count)
 


def build__claims_with_durations__df(dfs):
    print('\nHave entered function to add the duration columns to the dataframe.')
    df = dfs['claims']
    
    i = 1

    df['analyst_duration_touch_compliance'] = df.apply(lambda row: \
                                                       compute_analyst_duration_touch_compliance(row, dfs),
                                      axis=1)
        
    print('Have computed ' + str(i) + ' of the 18 additional duration functions')
    i = i+1

    df['recommended_dur_touch_count'] = df.apply(lambda row: compute_recommended_dur_touch_count(row, dfs),
                                                    axis=1)

    print('Have computed ' + str(i) + ' of the 18 additional duration functions')
    i = i+1

    df['actual_dur_touch_count'] = df.apply(lambda row: compute_actual_dur_touch_count(row, dfs),
                                           axis=1)

    print('Have computed ' + str(i) + ' of the 18 additional duration functions')
    i = i+1

    df['dur_touch_actual_v_rec_percentage'] = df.apply(lambda row: compute_dur_touch_actual_v_rec_percentage(row),
                                           axis=1)


    print('Have computed ' + str(i) + ' of the 18 additional duration functions')
    i = i+1

    df['payout_start_date'] = df.apply(lambda row: compute_payout_start_date(row),
                                       axis=1)

    print('Have computed ' + str(i) + ' of the 18 additional duration functions')
    i = i+1

    df['min_duration_days'] = df.apply(lambda row: compute_min_duration_days(row, dfs),
                                      axis=1)


    print('Have computed ' + str(i) + ' of the 18 additional duration functions')
    i = i+1

    df['actual_duration_days'] = df.apply(lambda row: compute_actual_duration_days(row),
                                      axis=1)

    print('Have computed ' + str(i) + ' of the 18 additional duration functions')
    i = i+1

    df['return_to_work_date'] = df.apply(lambda row: compute_return_to_work_date(row, dfs),
                                         axis=1)

    print('Have computed ' + str(i) + ' of the 18 additional duration functions')
    i = i+1

    for j in range(1,6):
        string = 'dur_touch_' + str(j) + '_target_date'
        df[string] = df.apply(lambda row: compute_dur_touch_target_date(j, row, dfs),
                                            axis=1)

        print('Have computed ' + str(i) + ' of the 18 additional duration functions')
        i = i+1

 
    for j in range(1,6):
        string = 'dur_touch_' + str(j) + '_actual_date'
        df[string] = df.apply(lambda row: compute_dur_touch_actual_date(j, row, dfs),
                              axis=1)

        print('Have computed ' + str(i) + ' of the 18 additional duration functions')
        i = i+1

    return df

####################################
#
#   writing the claims_with_durations dataframe into a postgres table
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
    print('\nEntered create__claims_with_durations__table')
    q = """
          CREATE TABLE claims_with_durations as
          SELECT *
          FROM claims_extended
          WHERE Claim_num < 0;
          
          ALTER TABLE claims_with_durations
            ADD CONSTRAINT Claim_num_for_durations_as_KEY 
              PRIMARY KEY (Claim_num);
          
          ALTER TABLE claims_with_durations
          ADD COLUMN analyst_duration_touch_compliance float8,
          ADD COLUMN recommended_dur_touch_count float8,        -- making this a float because Postgres does
                                                                --   null's in columns of type int
          ADD COLUMN actual_dur_touch_count float8,
          ADD COLUMN dur_touch_actual_v_rec_percentage	float8,
          ADD COLUMN payout_start_date date,
          ADD COLUMN return_to_work_date date,
          ADD COLUMN min_duration_days float8,
          ADD COLUMN actual_duration_days float8,	
          ADD COLUMN dur_touch_1_target_date date,	
          ADD COLUMN dur_touch_2_target_date date,			
          ADD COLUMN dur_touch_3_target_date date,		
          ADD COLUMN dur_touch_4_target_date date,		
          ADD COLUMN dur_touch_5_target_date date,		
          ADD COLUMN dur_touch_1_actual_date date,		
          ADD COLUMN dur_touch_2_actual_date date,		
          ADD COLUMN dur_touch_3_actual_date date,		
          ADD COLUMN dur_touch_4_actual_date date,		
          ADD COLUMN dur_touch_5_actual_date date
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

    
def push_df_into__claims_with_durations__table(df, db):
    print('\nHave entered function push_df_into__claims_with_durations__table')

    table_name = 'claims_with_durations'
    utils_postgres.load_df_into_table_with_same_columns(df, db, table_name)
    





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


    drop__diagnosis_duration_parameters__table(db)
    create__diagnosis_duration_parameters__table(db)
    import__diagnosis_duration_parameters_csv__into_postgres(db)
    df_dur = pull__diagnosis_duration_parameters__table_into_df(db)
    dfs['dur'] = df_dur
    # utils_general.display_df(df_dur)
    
    
    # drop__claims_analyst_parameters__table(db)
    # create__claims_analyst_parameters__table(db)
    # import__claims_analyst_parameters_csv__into_postgres(db)
    df_anal = pull__claims_analyst_parameters__table_into_df(db)
    dfs['anal'] = df_anal
    # utils_general.display_df(dfs['anal'])
    
    
    df_claims = pull__claims_extended__table_into_df(db)
    # df_claims = df_claims.head(20)
    dfs['claims'] = df_claims
    # utils_general.display_df(df_claims)
    
    df_dur = build__claims_with_durations__df(dfs)
    # utils_general.display_df(df_dur)
     
    drop__claims_with_durations__table(db)
    create__claims_with_durations__table(db)
    push_df_into__claims_with_durations__table(df_dur, db)

    print('\nWriting dataframe for claims_with_durations into csv file')
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    prefix = OPUS_DATA_OUTPUTS_DIR + timestamp + '__'
    df_dur.to_csv(prefix + 'claims_with_durations.csv', index=False)

    
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

    
    