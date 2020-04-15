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



def build_and_export_parameterized_cube(columns_list, db, timestamp):
    # the params should be a list of strings, where eaxh string
    # is the name of a group-able column of the table claims_agg
    q_template = """
          SELECT 
            {columns_param:} {comma_param:}
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
          {group_by_param:}
            {columns_param:}         
          {order_by_param:}
            {columns_param:}         
        """
    if len(columns_list) == 0:
        columns_string = ''
        comma_string = ''
        group_by_string = ''
        order_by_string = ''
    else:
        columns_string = columns_list[0]
        for i in range(1,len(columns_list)):
            columns_string = columns_string + ', ' + columns_list[i]
        comma_string = ','
        group_by_string = 'GROUP BY'
        order_by_string = 'ORDER BY'
    print(columns_string)
    q = q_template.format(columns_param = columns_string, 
                          comma_param = comma_string,
                          group_by_param = group_by_string,
                          order_by_param = order_by_string)
    print(q)
    utils_postgres.export_cube_query_to_csv(columns_list, q, db, timestamp)
    return
    


####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    
    start_datetime = datetime.now()
    # print('\nThis program starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))


    # print('Number of arguments: ', str(len(sys.argv)), ' arguments.')
    # print('Argument List:', str(sys.argv))
    
    
    permitted_columns = ['diagnosis',
                         'industry',
                         'geo',
                         'claims_analyst',
                         'igo_nigo',
                         'is_nurse_review_required',
                         'adherence_to_performance_guarantee',
                         'accuracy_of_decision']
    flag = False
    columns_list = []
    print()
    for i in range(1, len(sys.argv)):
        columns_list.append(sys.argv[i])
        # print(str(columns))
        if sys.argv[i] not in permitted_columns:
            flag = True
            print('The value "' + sys.argv[i] + '" is not a permitted column name for this query')

    if flag:
        print('\nThe set of permitted column names is: \n' + str(permitted_columns))
        print('Will exit the program now')
        sys.exit()
        
    print('You entered the following list of columns: \n' + str(columns_list))
        
            
        
    # open postgres connection with mimic database
    
    db = utils_postgres.connect_postgres()
 

    # columns = ['diagnosis', 'geo', 'igo_nigo', 'is_nurse_review_required', 'adherence_to_performance_guarantee', 'accuracy_of_decision']
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    build_and_export_parameterized_cube(columns_list, db, timestamp)
    

    # close connection to the mimic database    
    utils_postgres.close_postgres(db)


    end_datetime = datetime.now()
    duration_in_s = (end_datetime - start_datetime).total_seconds()
    seconds = utils_general.truncate(duration_in_s, 2)
    # minutes = utils_general.truncate(duration_in_s/60, 2) # divmod(duration_in_s, 60)[0] 
    # hours = utils_general.truncate(duration_in_s/3600, 2) # divmod(duration_in_s, 3600)[0]
    
    # print('\nThe build_cube script starting running at ' + start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    # print('It finished running at ' + end_datetime.strftime('%Y-%m-%d %H:%M:%S'))
    print('This execution of build_cube took ' + str(seconds) + ' seconds')
    # print('The duration in minutes was ' + str(minutes))
    # print('The duration in hours was  ' + str(hours))

    print('\nAs a convenience, we list here the set of permitted column names: \n' + str(permitted_columns))


