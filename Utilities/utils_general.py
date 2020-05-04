#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 00:19:34 2019

@author: rick
"""

from constants_used_for_insights_engine import *

from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta


import numpy as np


# def convert_py_time_to_iso_ET(pyTime):
#     return datetime.isoformat(pyTime) + '.000+05:00'

def print_current_time():
    print('Time now is: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

def truncate(f, n):
    '''Truncates/pads a float f to n decimal places without rounding'''
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return '.'.join([i, (d+'0'*n)[:n]])


def convert_string_date_to_date(d):
    return date(int(d[0:4]), int(d[5:7]), int(d[8:10]))
    # could also use: datetime.strptime(date_str, '%Y-%m-%d').date()

def display_df(df):
    # see https://thispointer.com/python-pandas-how-to-display-full-dataframe-i-e-print-all-rows-columns-without-truncation/
    #   to understand these and other display options
    # see also https://stackoverflow.com/questions/57860775/pandas-pd-options-display-max-rows-not-working-as-expected
    pd.set_option('display.max_columns', None)
    pd.set_option('display.min_rows', 30)
    print(df)


# assumes that dates are in format YYYY-MM-DD
def cal_days_between_dates(d1, d2):
     date1 = convert_string_date_to_date(d1)
     date2 = convert_string_date_to_date(d2)
     delta = date2 - date1
     return delta.days

def cal_days_offset(date_str, count):
    return str(convert_string_date_to_date(date_str) + pd.to_timedelta(count, unit='D')) 

# following second comment of 
#    https://stackoverflow.com/questions/546321/how-do-i-calculate-the-date-six-months-from-the-current-date-using-the-datetime
def date_n_cal_months_before(date_str, n):
    delta = 0 - n
    return str(convert_string_date_to_date(date_str) + relativedelta(months=delta))

def first_day_of_year_of_date(date_str):
    return str(convert_string_date_to_date(date_str).replace(month=1,day=1))

def first_day_of_month_of_date(date_str):
    return str(convert_string_date_to_date(date_str).replace(day=1))

def biz_day_on_or_immed_after(date_str):
    biz_date = np.busday_offset(date_str, 0, roll='forward', holidays=US_HOLIDAY_LIST)
    return str(biz_date)
    
# 4th comment in 
#  https://stackoverflow.com/questions/3615375/count-number-of-days-between-dates-ignoring-weekends
#  says that the input can be of form "YYYY-MM-DD" -- don't have to convert to datetime dates
#  (which is good, because pandas wasn't happy working with datetime dates)
def biz_days_between_dates(d1, d2):
     return int(np.busday_count(d1, d2, holidays=US_HOLIDAY_LIST))
 
# see https://docs.scipy.org/doc/numpy/reference/generated/numpy.busday_offset.html#numpy.busday_offset
#   for examples of the busday_offset with the roll='forward'
# this returns a date that is count biz days after the input date.
#    this works if count is negative
def biz_days_offset(date, count):
    date1 = np.busday_offset(date, 0, roll='forward', holidays=US_HOLIDAY_LIST)
    if not np.is_busday(date, holidays=US_HOLIDAY_LIST):
        print('\nWARNING: the date "' + str(date) + '" is not a business day; ' \
              + 'using the first business day immediately following that date,' \
              + 'namely, "' + str(date1) + '"\n')    
    biz_date = np.busday_offset(date1, count, holidays=US_HOLIDAY_LIST)
    # print('inside utils_general we have value: ' + str(biz_date))
    return str(biz_date)   
        
# used to take date2, which is x calendar days after date1 (which is typically 2019-11-01)
#   into the date2' which is x BUSINESS DAYS after date1
def convert_date2_after_date1_to_biz_date_after_date1(date1, date2):    
    count = cal_days_between_dates(date1, date2)
    biz_date = biz_days_offset(date1, count)
    return biz_date

 
    
    

####################################
#
#   main starts here
#
####################################
    
    
    
if __name__ == '__main__':
    
    # foo()
    pass

    '''
    timestamp = datetime(2198, 4, 22, 5, 0)
    print(timestamp)
    # timestampp = convert_py_time_to_IETF(timestamp)
    # print(timestampp)
    '''
    
    d1 = "2020-04-21"
    print(str(int(d1[0:4])))
    print(str(int(d1[5:7])))
    print(str(int(d1[8:10])))
    
    d1 = "2020-12-23"
    d2 = "2020-12-27"
    
    diff = cal_days_between_dates(d1, d2)
    print(type(diff))
    print(str(diff))
          
    biz_diff = biz_days_between_dates(d1, d2) 
    print(type(int(biz_diff)))
    print(str(biz_diff))
          
    
