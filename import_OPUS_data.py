#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 18:03:15 2019

@author: rick
"""

from constants_used_for_insights_engine import *

import util_general
import postgres_utilities

from datetime import datetime


import sys

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

def drop__claims__table(db):
    print('\nEntered drop__claims__table')
    q = " DROP TABLE claims "
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims" ')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table "claims" did not exist')

def drop__claims_raw__table(db):
    print('\nEntered drop__claims_raw__table')
    q = " DROP TABLE claims_raw "
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims" ')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table "claims" did not exist')
		
		
def create__claims_raw__table(db):
    print('\nEntered create__claims_raw__table')
    q = """
          CREATE TABLE claims_raw (
	        Claim_Control_No int4 CONSTRAINT Claim_Control_No_as_KEY PRIMARY KEY,
	        Claim_Value_Dollars int4,
	        Geo varchar,
	        Industry varchar,
	        Diagnosis varchar,
	        Claims_Analyst varchar,
	        Claims_per_hour_per_analyst float8,
	        Date_Received date,
	        IGO_NIGO varchar,
            Blank1 varchar,
	        Date_Follow_up_made date,
            Blank2 varchar,
	        Date_all_information_received date,
	        Is_nurse_review_required varchar,
            Blank3 varchar,
	        Date_Nurse_Decision_Made date,
            Blank4 varchar,
	        Date_Final_Decision_Made date,
	        Decision varchar,
            Blank5 varchar,
            Blank6 varchar,
	        Adherence_to_Performance_Guarantee varchar,
	        Accuracy_of_Decision varchar
          )
	     """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created table "claims_raw", including a commit')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Attempt to create table "claims_raw" has failed')

 
 
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




####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    
    print('\nWARNING: You may need to uncomment some commands from the main program of assemble_triples_table.py (and/or in some of the invoked scripts) in order to get the behaviors that you want.')
    
    start_datetime = datetime.now()
    print('\nThis program starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    
    # open postgres connection with mimic database
    db = postgres_utilities.connect_postgres()
        
    drop__claims__table(db)
    drop__claims_raw__table(db)
    create__claims_raw__table(db)
    import_csv_into__claims_raw__table(db)
    drop_columns_from__claims_raw__table(db)
    

    # close connection to the mimic database    
    postgres_utilities.close_postgres(db)
    
    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = util_general.truncate(duration_in_s, 2)
    minutes = util_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    hours = util_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    print('\nThe import_OPUS_data script started running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('The duration in seconds was ' + str(seconds))
    print('The duration in minutes was ' + str(minutes))
    print('The duration in hours was  ' + str(hours))
       
    
