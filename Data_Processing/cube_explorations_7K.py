#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 01:31:24 2020

@author: rick
"""

import utils_general
from constants_used_for_insights_engine import *
import utils_postgres


from datetime import datetime

import sys

import psycopg2
import psycopg2.extras

import pandas as pd
# see pandas 1.0.3 api reference guid:
#   https://pandas.pydata.org/pandas-docs/stable/reference/index.html
#   older pandas: see https://pandas.pydata.org/pandas-docs/version/0.13/api.html, search for sql
import pandas.io.sql as psql


####################################
#
#   Building a CUBE-oriented table from claims_agg table
#
####################################

def drop__claims_cube__table(db):
    print('\nEntered drop__claims_cube__table')
    q = " DROP TABLE claims_cube"
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Dropped the table "claims_cube" ')
    except:
        # if the attempt to drop table failed, then you have to rollback that request
        db['conn'].rollback()
        print('Table "claims_cube" did not exist')


def build__claims_cube__table(db):
    print('\nEntered function build__claims_cube__table')
    q = """
          CREATE TABLE claims_cube AS
          SELECT 
            diagnosis, industry, geo, 
            claims_analyst, igo_nigo,
            is_nurse_review_required, adherence_to_performance_guarantee,
            accuracy_of_decision,
            --
            sum(total_count) as total_count,
            SUM(total_over_five_biz_days) as total_over_five_biz_days,
            SUM(total_over_ten_biz_days) as total_over_ten_biz_days,
            ROUND((SUM(avg_claim_value_dollars * total_count)/SUM(total_count))::numeric, 2) as avg_claim_value_dollars,
            ROUND((SUM(avg_hours_per_claim * total_count)/SUM(total_count))::numeric, 2) as avg_hours_per_claim,
            ROUND((SUM(avg_nigo_follow_up_biz_days * total_count)/SUM(total_count))::numeric, 2) as avg_nigo_follow_up_biz_days,
            ROUND((SUM(avg_nigo_requested_to_all_info_biz_days * total_count)/SUM(total_count))::numeric, 2) as avg_nigo_requested_to_all_info_biz_days,
            ROUND((SUM(avg_nigo_received_to_all_info_biz_days * total_count)/SUM(total_count))::numeric, 2) as avg_nigo_received_to_all_info_biz_days,
            ROUND((SUM(avg_nurse_decision_biz_days * total_count)/SUM(total_count))::numeric, 2) as avg_nurse_decision_biz_days,
            ROUND((SUM(avg_total_biz_days * total_count)/SUM(total_count))::numeric, 2) as avg_total_biz_days
          FROM claims_agg
          GROUP BY
            GROUPING SETS (
                   (diagnosis),
                   (industry),
                   (geo),
                   (claims_analyst),
                   (diagnosis, claims_analyst),
                   (diagnosis, igo_nigo),
                   (is_nurse_review_required),
                   (diagnosis, is_nurse_review_required),
                   (diagnosis, is_nurse_review_required, accuracy_of_decision),
                   (diagnosis, adherence_to_performance_guarantee)
                )
         ORDER BY
            diagnosis, industry, geo, 
            claims_analyst, igo_nigo,
            is_nurse_review_required, adherence_to_performance_guarantee,
            accuracy_of_decision
          
        """
    try:
        db['cursor'].execute(q)
        db['conn'].commit()
        print('Created the table "claims_cube" ')
    except Exception as e:
            db['conn'].rollback()
            print('  Failed to create table "claims_cube" ')
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
    

    # print('Number of arguments: ', str(len(sys.argv)), ' arguments.')
    # print('Argument List:', str(sys.argv))
    

    start_datetime = datetime.now()
    print('\nThis program starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))

    
    # open postgres connection with mimic database
    db = utils_postgres.connect_postgres()
    


    drop__claims_cube__table(db)
    build__claims_cube__table(db)


    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    utils_postgres.export_table_to_csv('claims_extended', db, timestamp)
    utils_postgress.export_table_to_csv('claims_agg', db, timestamp)
    utils_postgres.export_table_to_csv('claims_cube', db, timestamp)


    # close connection to the mimic database    
    utils_postgres.close_postgres(db)
    
    

    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = utils_general.truncate(duration_in_s, 2)
    minutes = utils_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    hours = utils_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    print('\nThe cube_explorations script starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('The duration in seconds was ' + str(seconds))
    # print('The duration in minutes was ' + str(minutes))
    # print('The duration in hours was  ' + str(hours))


    


