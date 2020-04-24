#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 00:33:49 2020

@author: rick
"""

from constants_used_for_insights_engine import *
import utils_general
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
#   the INVENTORY function
#
####################################

'''
Stages are:
    igo_nigo_switch (an automated stage, so happens "immediately")
    nigo_following_up (in the case of nigo)
    waiting_for_nigo
    deciding_1 (either reaches a final decision of approve/reject or that a nurse review is needed)
    nurse_deciding
    deciding_2
    paying_out    
'''

def build_parameterized_inventory_query():
    print('\nHave entered the function that creates query template for the inventory queries')
    q = """
(select '1_following_up' as stage, count (*)
 from claims_with_durations c
 where c.igo_nigo = 'NIGO' 
   and c.received_date < '{date_str:}'
   and '{date_str:}' <= c.nigo_followed_up_date) 
union 
(select '2_waiting_call' as stage, count (*)
 from claims_with_durations c
 where c.igo_nigo = 'NIGO'
   and c.nigo_followed_up_mode = 'Call'
   and c.nigo_followed_up_date < '{date_str:}' 
   and '{date_str:}' <= c.all_info_received_date)
union
(select '3_waiting_email' as stage, count (*)
 from claims_with_durations c
 where c.igo_nigo = 'NIGO'
   and c.nigo_followed_up_mode = 'Email'
   and c.nigo_followed_up_date < '{date_str:}'
   and '{date_str:}' <= c.all_info_received_date)
union
(select '4_deciding_1' as stage, count (*)
 from claims_with_durations c
 where ( c.igo_nigo = 'IGO'
         and c.received_date < '{date_str:}'
         and '{date_str:}' <= c.decided_1_date )      
    or ( c.igo_nigo = 'NIGO'
         and c.all_info_received_date < '{date_str:}'
         and '{date_str:}' <= c.decided_1_date ) )
union 
(select '5_nurse_deciding' as stage, count (*)
 from claims_with_durations c
 where c.is_nurse_review_required = 'Yes'
   and c.decided_1_date < '{date_str:}'
   and '{date_str:}' <= c.nurse_reviewed_date )        
union 
(select '6_deciding_2' as stage, count (*)
 from claims_with_durations c
 where c.is_nurse_review_required = 'Yes'
   and c.nurse_reviewed_date < '{date_str:}'
   and '{date_str:}' <= c.decided_2_date )        
union
(select '7_paying_out' as stage, count (*)
 from claims_with_durations c
 where ( c.decision = 'Approved'
         and c.is_nurse_review_required = 'No'
         and c.decided_1_date < '{date_str:}'
         and '{date_str:}' < c.return_to_work_date ) 
    or ( c.decision = 'Approved'
         and c.is_nurse_review_required = 'Yes'
         and c.decided_2_date < '{date_str:}'
         and '{date_str:}' < c.return_to_work_date ) )
union
(select '8_total' as stage, count(*)
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
        and '{date_str:}' <= c.decided_1_date) )   
order by stage
        """
    return q
    
 
def inventory_query(db, date):
    print('\nHave entered the inventory_query function')
    
    q = build_parameterized_inventory_query()

    q = q.format(date_str = date)
    print('\nThe query with date plugged in is:\n')
    print(q)
    print()
    timestamp = datetime.now().strftime('%Y-%m-%d--%H-%M') 
    filenameroot = 'inventory_as_of__' + date 
    utils_postgres.export_query_to_csv(db, q, timestamp, filenameroot)   


def compute_inventory(db, list):
    date = list[0]
    print('\nHave entered compute_inventory function with input date: ' + date)
    biz_date = utils_general.biz_day_on_or_immed_after(date)
    if date != biz_date:
        print('\nNote: The date "' + date + '" is not a business day')
    inventory_query(db, date)
    
   
####################################
#
#   the AGG function
#
####################################


def compute_agg(db, list):
    print('\nHave entered compute_agg function')


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
    
    permitted_functions = ['inventory', 'agg']
    
    permitted_columns = ['diagnosis',
                         'industry',
                         'geo',
                         'claims_analyst',
                         'igo_nigo',
                         'is_nurse_review_required',
                         'accuracy_of_decision']
    flag = False
    
    param_list = []
    # param_list = ['inventory', '2019-12-20']
    for i in range(1, len(sys.argv)):
        param_list.append(sys.argv[i])
    print('You entered the function name "', param_list[0], '", and parameters ', param_list[1:])
    
    # open postgres connection with mimic database
    db = utils_postgres.connect_postgres()    
    
    
    if param_list[0] == 'inventory':
        compute_inventory(db, param_list[1:])
    elif param_list[0] == 'agg':
        compute_agg(db, param_list[1:])
    else:
        print('\nThe first parameter must be one of the permitted functions, i.e., one of ' + permitted_functions)
    
    

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
    
    '''

    print('\nWhen invoking this function, please provide:')
    print('    function name (choices are: inventory (on a date), agg for claims received in a period)')
    print('    and for the inventory query please provide a date (if not a business day we will use next business day)')
    print('    and for the agg query please provide start- and end-date; you may also include one or more of the permitted columns from this list:')
    print('    ' + str(permitted_columns))

    '''

