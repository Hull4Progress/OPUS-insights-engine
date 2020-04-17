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



    

####################################
#
#   extending claims_extended with the duration columns
#
####################################

def import__claims_extended__join__diagnosis_duration_parameters___into_df(db): 
    pass


def drop__claims_extended_durations__table(db):
    print('\nEntered drop__claims_extended_durations__table')
    q = " DROP TABLE claims_extended_durations"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_extended_durations" ')
    except Exception as e:  # if you don't want the exception comment, then drop "Exception as e"
        db['conn'].rollback()
        print('Failed to drop table "claims_extended_durations", perhaps because it did not exist')
        # """
        # to use this part, also adjust the "except" line 3 lines above
        print('  The exception error message is as follows:')
        if hasattr(e, 'message'):
            print(e.message)
        else:
            print(e)
        # """

def create__claims_extended_durations__table(db):
    print('\nEntered create__claims_extended_duration__table')
    q = """
          CREATE TABLE duration_parameters as
          SELECT *
          FROM claims_extended
          WHERE Claim_num < 0;
          
          ALTER TABLE duration_parameters
            ADD CONSTRAINT Claim_num_for_durations_as_KEY 
              PRIMARY KEY (Claim_num);
          
          ALTER TABLE duration_parameters
          ADD COLUMN 
          ADD COLUMN 
          ADD COLUMN 
          ADD COLUMN 
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

    

    
def populate__claims_extended_durations__table(db):
    pass
    





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


    drop__diagnosis_duration_parameters__table(db)
    create__diagnosis_duration_parameters__table(db)
    import__diagnosis_duration_parameters_csv__into_postgres(db)
    
    '''
    df_claims = import__claims_extended__into_df(db)
    drop__claims_extended_durations__table(db)
    create__claims_extended_durations__table(db)
    
    import__claims_extended__join__diagnosis_duration_parameters___into_df(db)
 
    populate__claims_extended_durations__table(db)
    '''
    
    
 
    
    # close connection to the mimic database    
    utils_postgres.close_postgres(db)
 
    
    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = utils_general.truncate(duration_in_s, 2)
    # minutes = utils_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    # hours = utils_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    print('\nThe script to augment claims_extended with duration sdata started running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('The duration in seconds was ' + str(seconds))
    # print('The duration in minutes was ' + str(minutes))
    # print('The duration in hours was  ' + str(hours))

    
    