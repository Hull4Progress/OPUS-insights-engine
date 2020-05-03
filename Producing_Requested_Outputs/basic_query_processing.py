#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 00:33:49 2020

@author: rick
"""

import sys

sys.path.append('../Constants')
from constants_used_for_insights_engine import *

sys.path.append('../Utilities')
import utils_general
import utils_postgres

from datetime import datetime


import psycopg2
import psycopg2.extras

import pandas as pd
# see pandas 1.0.3 api reference guid:
#   https://pandas.pydata.org/pandas-docs/stable/reference/index.html
#   older pandas: see https://pandas.pydata.org/pandas-docs/version/0.13/api.html, search for sql
import pandas.io.sql as psql


####################################
#
#   global parameters for this file
#
####################################


permitted_functions = ['inventory', 'inv-agg', 'agg-TAT', 'agg-gem']
permitted_functions_with_descriptions = [
        '- inventory: gives count of inventory for a given date, broken by stage',
        '- inv-agg: gives count about the inventory for a given date, broken by stage and other categories',
        '- agg-TAT: gives aggregate TAT information for a set of claims decided between 2 dates, broken by selected categories',
        '- agg-gen: gives aggregate TAT information for a set of claims decided between 2 dates, broken by selected categories']
    
permitted_columns = ['diagnosis',
                         'industry',
                         'geo',
                         'claims_analyst',
                         'igo_nigo',
                         'is_nurse_review_required',
                         'accuracy_of_decision']
    
suggested_dates = ['2019-11-15', '2020-03-01']

####################################
#
#   the INVENTORY function
#
####################################

'''
Stages are:
    intake_deciding 
    nigo_following_up (in the case of nigo)
    waiting_for_nigo
    deciding_1 (either reaches a final decision of approve/reject or that a nurse review is needed)
    nurse_deciding
    deciding_2
    paying_out    
'''

def build_parameterized_inv_agg_query():
    print('\nHave entered the function that creates query template for the inventory and inv_agg queries')
    q = """
(select '0_intake_deciding' as stage, {group_cols:}{group_comma:} count (*)
 from claims_with_durations c
 where c.received_date < '{date_str:}'
   and '{date_str:}' <= c.intake_decided_date
 {group_by:} {group_cols:} )
union 
(select '1_following_up' as stage, {group_cols:}{group_comma:} count (*)
 from claims_with_durations c
 where c.igo_nigo = 'NIGO' 
   and c.intake_decided_date < '{date_str:}'
   and '{date_str:}' <= c.nigo_followed_up_date
 {group_by:} {group_cols:} )
union 
(select '2_waiting_call' as stage, {group_cols:}{group_comma:} count (*)
 from claims_with_durations c
 where c.igo_nigo = 'NIGO'
   and c.nigo_followed_up_mode = 'Call'
   and c.nigo_followed_up_date < '{date_str:}' 
   and '{date_str:}' <= c.all_info_received_date
 {group_by:} {group_cols:} )
union
(select '3_waiting_email' as stage, {group_cols:}{group_comma:} count (*)
 from claims_with_durations c
 where c.igo_nigo = 'NIGO'
   and c.nigo_followed_up_mode = 'Email'
   and c.nigo_followed_up_date < '{date_str:}'
   and '{date_str:}' <= c.all_info_received_date
 {group_by:} {group_cols:} )
union
(select '4_deciding_1' as stage, {group_cols:}{group_comma:} count (*)
 from claims_with_durations c
 where ( c.igo_nigo = 'IGO'
         and c.intake_decided_date < '{date_str:}'
         and '{date_str:}' <= c.decided_1_date )      
    or ( c.igo_nigo = 'NIGO'
         and c.all_info_received_date < '{date_str:}'
         and '{date_str:}' <= c.decided_1_date ) 
 {group_by:} {group_cols:} )
union 
(select '5_nurse_deciding' as stage, {group_cols:}{group_comma:} count (*)
 from claims_with_durations c
 where c.is_nurse_review_required = 'Yes'
   and c.decided_1_date < '{date_str:}'
   and '{date_str:}' <= c.nurse_reviewed_date    
 {group_by:} {group_cols:} )
union 
(select '6_deciding_2' as stage, {group_cols:}{group_comma:} count (*)
 from claims_with_durations c
 where c.is_nurse_review_required = 'Yes'
   and c.nurse_reviewed_date < '{date_str:}'
   and '{date_str:}' <= c.decided_2_date 
 {group_by:} {group_cols:} )      
union
(select '7_paying_out' as stage, {group_cols:}{group_comma:} count (*)
 from claims_with_durations c
 where ( c.decision = 'Approved'
         and c.is_nurse_review_required = 'No'
         and c.decided_1_date < '{date_str:}'
         and '{date_str:}' < c.return_to_work_date ) 
    or ( c.decision = 'Approved'
         and c.is_nurse_review_required = 'Yes'
         and c.decided_2_date < '{date_str:}'
         and '{date_str:}' < c.return_to_work_date ) 
 {group_by:} {group_cols:} )
union
(select '8_total' as stage, {group_cols:}{group_comma:} count(*)
 from claims_with_durations c
 where (c.decision = 'Approved'
        and c.received_date < '{date_str:}'
        and ('{date_str:}' < c.return_to_work_date   -- in some cases return_to_work_date is < final decision date
             or '{date_str:}' <= c.decided_1_date
             or '{date_str:}' <= c.decided_2_date))
    or (c.decision = 'Declined'
        and c.is_nurse_review_required = 'Yes'
        and c.received_date < '{date_str:}'
        and '{date_str:}' <= c.decided_2_date)
    or (c.decision = 'Declined'
        and c.is_nurse_review_required = 'No'
        and c.received_date < '{date_str:}'
        and '{date_str:}' <= c.decided_1_date) 
 {group_by:} {group_cols:} )
{order_by:}{group_cols:}
{order_by_for_inventory:}

        """
    return q
   
 
def inventory_query(db, list, csv_or_json):
    print('\nHave entered the inventory_query function')
    date = list[0]
    # q = build_parameterized_inventory_query()
    # q = q.format(date_str = date)
    q = build_parameterized_inv_agg_query()
    q = q.format(date_str = date, 
                 group_cols = '',
                 group_comma = '',
                 group_by = '',
                 order_by = '',
                 order_by_for_inventory = 'ORDER BY stage')

    print('\nThe query with date plugged in is:\n')
    print(q)
    print()
    
    if csv_or_json == 'csv':
        timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
        filenameroot = 'inventory_as_of__' + date 
        utils_postgres.export_query_to_csv(db, q, timestamp, filenameroot)  
        return
    elif csv_or_json == 'json':
        return utils_postgres.export_query_to_json(db, q) 
    else:
        print('ERROR: you need to specify kind of output desired as "csv" or "json"')
        return
        


# not using this one currently
def compute_inventory(db, list):
    date = list[0]
    print('\nHave entered compute_inventory function with input date: ' + date)
    biz_date = utils_general.biz_day_on_or_immed_after(date)
    if date != biz_date:
        print('\nNote: The date "' + date + '" is not a business day')
    inventory_query(db, list)
    
def valid_inventory_input(list, suggested_dates):
    try:
        biz_date = utils_general.biz_day_on_or_immed_after(list[1])
    except:
        print('\n====> You entered the date ' + list[1])
        print('====> This is not a valid calendar date\n') 
        return False
    if list[1] < suggested_dates[0] or list[1] > suggested_dates[1]:
        print('\n====> You entered the date ' + list[1])
        print('====> Please use a date that is between ' + suggested_dates[0] + ' and ' + suggested_dates[1])
        print()
        return False
    return True



####################################
#
#   the INV_AGG function
#
####################################

    
 

def inv_agg_query(db, list, csv_or_json):
    print('\nHave entered inv_agg_query function')
    date = list[0]
    if len(list) == 1:   # this means there are no columns to do group-by aggregation on
        compute_inventory(db, list)
        sys.exit()
    cols_comma = list[1]    # the list should start without a comma and should end without a comma
    for i in range(2, len(list)):
        cols_comma = cols_comma + ', ' + list[i] + ' '
    print('\nThe list of columns is:', cols_comma)
    q = build_parameterized_inv_agg_query()
    q = q.format(date_str = date, 
                 group_cols = cols_comma,
                 group_comma = ',',
                 group_by = 'GROUP BY',
                 order_by = 'ORDER BY stage, ',
                 order_by_for_inventory = '')
    print('\nThe query with date and columns plugged in is:\n')
    print(q)
    print()
    if csv_or_json == 'csv':
        cols_dash = list[1]
        for i in range(2, len(list)):
            cols_dash = cols_dash + '-' + list[i]
        print('\nThe list of columns separated with dashes is:', cols_dash)
    
        timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
        filenameroot = 'inv_agg_TAT_for__' + cols_dash + '__as_of__' + date 
        utils_postgres.export_query_to_csv(db, q, timestamp, filenameroot)   
        return
    elif csv_or_json == 'json':
        return utils_postgres.export_query_to_json(db, q) 
    else:
        print('ERROR: you need to specify kind of output desired as "csv" or "json"')
        return
 

    
# not using this one currently
def compute_inv_agg(db, list):
    date = list[0]
    print('\nHave entered compute_inv_agg function with input date: ' + date)
    biz_date = utils_general.biz_day_on_or_immed_after(date)
    if date != biz_date:
        print('\nNote: The date "' + date + '" is not a business day')
    inv_agg_query(db, list)
    
def valid_inv_agg_input(list, suggested_dates, permitted_columns):
    try:
        biz_date = utils_general.biz_day_on_or_immed_after(list[1])
    except:
        print('\n====> You entered the date ' + list[1])
        print('====> This is not a valid calendar date\n') 
        return False
    if list[1] < suggested_dates[0] or list[1] > suggested_dates[1]:
        print('\n====> You entered the date ' + list[1])
        print('====> Please use a date that is between ' + suggested_dates[0] + ' and ' + suggested_dates[1])
        print()
        return False
    for i in range(2, len(list)):
        if list[i] not in permitted_columns:
            print('\nYou included a column name "' + list[i] + '", but this is not a permitted column name')
            print('====> The set of permitted column names is: ')
            print('      ', permitted_columns)
            print()
            return False
    return True
    

####################################
#
#   the AGG_TAT and AGG_GEN function
#
####################################

def build_parameterized_agg_query(switch):
    print('\nHave entered the function that creates query template for the agg_TAT and agg_gen queries')
    if switch == 'TAT':
        q = """
select {cols_list}{comma:}
       count(*) as count_of_claims,
       ROUND(AVG(total_biz_days), 2)::float as avg_TAT, 
       sum(over_five_biz_days) as count_tat_over_5_days, 
       ROUND(((100 * sum(over_five_biz_days))::float/count(*))::numeric, 2)::float as percent_tat_over_5_days,
       sum(over_ten_biz_days) as count_tat_over_10_days,
       ROUND((( 100 * sum(over_ten_biz_days))::float / count(*))::numeric, 2)::float as percent_tat_over_10_days
from claims_with_durations
where
   '{date_start:}' <= decided_2_date
   and decided_2_date <= '{date_end:}'
{group_by:} {cols_list:}
{order_by:} {cols_list:}
            """
    else:
        q = """
select {cols_list}{comma:}
       count(*) as count_of_claims,
       ROUND(AVG(total_biz_days), 2)::float as avg_TAT, 
       sum(over_five_biz_days) as count_tat_over_5_days, 
       (100 * sum(over_five_biz_days))/count(*) as percent_tat_over_5_days,
       sum(over_ten_biz_days) as count_tat_over_10_days,
       (( 100 * sum(over_ten_biz_days)) / count(*)) as percent_tat_over_10_days,
       sum(nigo_followed_up_to_all_info_received_biz_days) as total_nigo_follow_up_days,
       (select count(*)
               from claims_with_durations c1
               where  '{date_start:}' <= c1.decided_2_date
                 and c1.decided_2_date <= '{date_end:}'
                 and c1.igo_nigo = 'NIGO' {and_cond_list:} )
          as total_nigo_count,
       ROUND((sum(nigo_followed_up_to_all_info_received_biz_days)::float /
                 (0.001 + (select count(*)
                  from claims_with_durations c1
                  where  '{date_start:}' <= c1.decided_2_date
                      and c1.decided_2_date <= '{date_end:}'
                      and c1.igo_nigo = 'NIGO' {and_cond_list:}
                      )))::numeric, 2)::float
                as avg_nigo_follow_up_days,
       sum(total_analyst_hours) as total_analyst_hours,
       ROUND(AVG(total_analyst_hours)::numeric,2)::float as avg_analyst_hours
from claims_with_durations c
where
   '{date_start:}' <= decided_2_date
   and decided_2_date <= '{date_end:}'
{group_by:} {cols_list:}
{order_by:} {cols_list:}

            """

    return q

def agg_query(db, list, switch, csv_or_json):
    print('\nHave entered agg_gen_query function')
    date1 = list[0]
    date2 = list[1]
    if len(list) == 2:
        cols_comma = ''
    else:
        cols_comma = list[2]    # the list should start without a comma and should end without a comma
        for i in range(3, len(list)):
            cols_comma = cols_comma + ', ' + list[i] + ' '
    print('\nThe list of columns is:', cols_comma)
    q = build_parameterized_agg_query(switch)
    if switch == 'TAT':
        if len(list) == 2:   # in this case, no group-by columns
            q = q.format(cols_list = '',
                     comma = '',
                     date_start = date1,
                     date_end = date2,
                     group_by = '',
                     order_by = ''                     
            )
        else:
            q = q.format(cols_list = cols_comma,
                     comma = ', ',
                     date_start = date1,
                     date_end = date2,
                     group_by = 'GROUP BY',
                     order_by = 'ORDER BY'                     
            )
    else:  # in this case switch == 'gen'
        if len(list) == 2:   # in this case, no group-by columns
            q = q.format(cols_list = '',
                     comma = '',
                     date_start = date1,
                     date_end = date2,
                     group_by = '',
                     order_by = '',
                     and_cond_list = ''
            )
        else:
            and_list = ''
            for i in range(2, len(list)):
                and_list = and_list + '\n                      and c1.' + list[i] + ' = c.' + list[i]                    
            q = q.format(cols_list = cols_comma,
                     comma = ', ',
                     date_start = date1,
                     date_end = date2,
                     group_by = 'GROUP BY',
                     order_by = 'ORDER BY',
                     and_cond_list = and_list                     
            )
        
    print('\nThe query with dates and columns plugged in is:\n')
    print(q)
    print()
    if csv_or_json == 'csv':
        if len(list) == 2:
            cols_dash = 'no-group-by-columns-selected'
        else:
            cols_dash = list[2]
            for i in range(3, len(list)):
                cols_dash = cols_dash + '-' + list[i]
        print('\nThe list of columns separated with dashes is:', cols_dash)
    
        timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
        filenameroot = 'agg_' + switch + '_for__' + cols_dash + '__for_claims_decided_between_' + date1 + '_and_' + date2
        utils_postgres.export_query_to_csv(db, q, timestamp, filenameroot)
    elif csv_or_json == 'json':
        return utils_postgres.export_query_to_json(db, q) 
    else:
        print('ERROR: you need to specify kind of output desired as "csv" or "json"')
        return

# not using this one currently
def compute_agg(db, list, switch):
    date1 = list[0]
    date2 = list[1]
    print('\nHave entered compute_agg_gen function with input dates: ' + date1 + ' and ' + date2)
    biz_date1 = utils_general.biz_day_on_or_immed_after(date1)
    if date1 != biz_date1:
        print('\nNote: The date "' + date1 + '" is not a business day')
    biz_date2 = utils_general.biz_day_on_or_immed_after(date2)
    if date2 != biz_date2:
        print('\nNote: The date "' + date2 + '" is not a business day')
    agg_query(db, list, switch)



def valid_agg_input(list, suggested_dates, permitted_columns):
    if len(list) < 2:
        print('\n====> For this query you need to provide 2 dates, which provide the bounds on the decision dates of the claims to be considered')
        return False
    try:
        biz_date = utils_general.biz_day_on_or_immed_after(list[1])
    except:
        print('\n====> You entered the date ' + list[1])
        print('====> This is not a valid calendar date\n') 
        return False
    try:
        biz_date = utils_general.biz_day_on_or_immed_after(list[2])
    except:
        print('\n====> You entered the date ' + list[2])
        print('====> This is not a valid calendar date\n') 
        return False
    if list[2] < list[1]:
        print('\n====> You entered the dates', list[1], list[2])
        print('====> Please provide 2 dates where the first is <= the second\n')            
        return False
    if list[1] < suggested_dates[0] or list[1] > suggested_dates[1] \
            or list[2] < suggested_dates[0] or list[2] > suggested_dates[1]:
        print('\n====> You entered the dates', list[1], list[2])
        print('====> Please use dates that are between ' + suggested_dates[0] + ' and ' + suggested_dates[1])
        print()
        return False
    for i in range(3, len(list)):
        if list[i] not in permitted_columns:
            print('\nYou included a column name "' + list[i] + '", but this is not a permitted column name')
            print('====> The set of permitted column names is: ')
            print('      ', permitted_columns)
            print()
            return False
    return True


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
    
    
    flag = False
    
    param_list = []
    # param_list = ['inventory', '2019-12-20']
    for i in range(1, len(sys.argv)):
        param_list.append(sys.argv[i])
    print('You entered the function name "', param_list[0], '", and parameters ', param_list[1:])
        
    
    if param_list[0] == 'inventory':
        if not valid_inventory_input(param_list, suggested_dates):
            sys.exit()
        # open postgres connection with mimic database
        db = utils_postgres.connect_postgres()    
        inventory_query(db, param_list[1:], 'csv')
        # close connection to the mimic database    
        utils_postgres.close_postgres(db)
    elif param_list[0] == 'inv-agg':
        if not valid_inv_agg_input(param_list, suggested_dates, permitted_columns):
            sys.exit()
        # open postgres connection with mimic database
        db = utils_postgres.connect_postgres()    
        inv_agg_query(db, param_list[1:], 'csv')
        # close connection to the mimic database    
        utils_postgres.close_postgres(db)
    elif param_list[0] == 'agg-TAT':
        if not valid_agg_input(param_list, suggested_dates, permitted_columns):
            sys.exit()
        # open postgres connection with mimic database
        db = utils_postgres.connect_postgres() 
        switch = 'TAT'
        agg_query(db, param_list[1:], switch, 'csv')
        # close connection to the mimic database    
        utils_postgres.close_postgres(db)
    elif param_list[0] == 'agg-gen':
        if not valid_agg_input(param_list, suggested_dates, permitted_columns):
            sys.exit()
        # open postgres connection with mimic database
        db = utils_postgres.connect_postgres() 
        switch = 'gen'
        agg_query(db, param_list[1:], switch, 'csv')
        # close connection to the mimic database    
        utils_postgres.close_postgres(db)
    else:
        print('\nThe first parameter must be one of the permitted functions, i.e., one of: ') 
        for func in permitted_functions_with_descriptions:
            print(func)
    
    



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
    
    print()
    
    '''

    print('\nWhen invoking this function, please provide:')
    print('    function name (choices are: inventory (on a date), agg for claims received in a period)')
    print('    and for the inventory query please provide a date (if not a business day we will use next business day)')
    print('    and for the agg query please provide start- and end-date; you may also include one or more of the permitted columns from this list:')
    print('    ' + str(permitted_columns))

    '''

