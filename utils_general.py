#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 00:19:34 2019

@author: rick
"""

from constants_used_for_insights_engine import *

from datetime import datetime
from datetime import date

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

# assumes that dates are in format YYYY-MM-DD

def days_between_dates(d1, d2):
     date1 = convert_string_date_to_date(d1)
     date2 = convert_string_date_to_date(d2)
     delta = date2 - date1
     return delta.days
 
# 4th comment in 
#  https://stackoverflow.com/questions/3615375/count-number-of-days-between-dates-ignoring-weekends
#  says that the input can be of form "YYYY-MM-DD" -- don't have to convert to datetime dates
#  (which is good, because pandas wasn't happy working with datetime dates)
def biz_days_between_dates(d1, d2):
     return int(np.busday_count(d1, d2, holidays=US_HOLIDAY_LIST))
 
# see https://docs.scipy.org/doc/numpy/reference/generated/numpy.busday_offset.html#numpy.busday_offset
#   for examples of the busday_offset with the roll='forward'
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
    count = days_between_dates(date1, date2)
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
    
    diff = days_between_dates(d1, d2)
    print(type(diff))
    print(str(diff))
          
    biz_diff = biz_days_between_dates(d1, d2) 
    print(type(int(biz_diff)))
    print(str(biz_diff))
          
    
