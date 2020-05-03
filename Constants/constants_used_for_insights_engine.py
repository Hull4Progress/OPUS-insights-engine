#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 17:03:10 2019

@author: rick 
"""

import pandas as pd
import numpy as np

# if you need to add holidays to the USFederalHolidayCalendar then see
#     https://stackoverflow.com/questions/33094297/create-trading-holiday-calendar-with-pandas
from pandas.tseries.holiday import get_calendar  #, HolidayCalendarFactory, GoodFriday
from datetime import datetime

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay



# This file holds constants used in the routines for assembling measurement-treatment-measurement TRIPLES


POSTGRES_SIGN_IN_CREDENTIALS_FOR_PSYCOPG2 = "dbname='opus' user='rick' host='localhost' password='postgres' port='5432' "
# POSTGRES_SIGN_IN_CREDENTIALS = "dbname='mimic' user='postgres' host='localhost' password='elisas96' port='5432' "


DB_ADDRESS_FOR_SQLALCHEMY = "postgresql://rick:postgres@localhost:5432/opus"


# location of synthetic claims data created by Ramesh
# this is old data: RAMESH_CLAIMS_CSV_7K = "../OPUS_DATA/2020-01-18--Opus--Ramesh-example-data.csv"

# with 2020-04-13b a stray value in 'Salary per month' was fixed,
#   and all dates were put into YYYY-MM-DD format
RAMESH_CLAIMS_CSV_5K = "../Raw_Data/2020-04-13b--Total---5386.csv"

DIAGNOSIS_DURATION_PARAMETERS = "../Raw_Data/Diagnosis_Duration_Parameters_v01.csv"

CLAIMS_ANALYST_PARAMETERS = "../Raw_Data/Claims_Analyst_Parameters_v01.csv"

OPUS_DATA_OUTPUTS_DIR = "../../OPUS_DATA/DATA_OUTPUTS/"


# adopted from https://stackoverflow.com/questions/30265711/python-pandas-numpy-direct-calculation-of-number-of-business-days-between
# so that I can use this list as  numpy.busday.offset as in
#   https://docs.scipy.org/doc/numpy/reference/generated/numpy.busday_offset.html
US_HOLIDAY_LIST = USFederalHolidayCalendar().holidays('2019-01-01', '2021-12-31').date.tolist()
'''
The set of US federal holidays between 2019-01-01 and 2021-12-31, as computed above is:
              ['2019-01-01', '2019-01-21', '2019-02-18', '2019-05-27',
               '2019-07-04', '2019-09-02', '2019-10-14', '2019-11-11',
               '2019-11-28', '2019-12-25', '2020-01-01', '2020-01-20',
               '2020-02-17', '2020-05-25', '2020-07-03', '2020-09-07',
               '2020-10-12', '2020-11-11', '2020-11-26', '2020-12-25',
               '2021-01-01', '2021-01-18', '2021-02-15', '2021-05-31',
               '2021-07-05', '2021-09-06', '2021-10-11', '2021-11-11',
               '2021-11-25', '2021-12-24', '2021-12-31']
'''

DEFAULT_DATE_FOR_TODAY = '2020-02-20'



####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    pass