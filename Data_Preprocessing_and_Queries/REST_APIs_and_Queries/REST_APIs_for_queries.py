#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 23:17:45 2020

@author: rick
"""

import sys

# actually, this didn't resolve the problem
# had trouble with serializing some decimals into JSON if using normal json,
#   so switched to simplejson as per one of the comments in
#   https://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object
#   (I did have to import simplejson)
# import simplejson as json

import json

import sys

sys.path.append('../Constants')
from constants_used_for_insights_engine import *

sys.path.append('../Utilities')
import utils_general
import utils_postgres

import flask
from flask import request, jsonify


import basic_query_processing

####################################
#
#   defn for class Request
#
####################################


class Request():
    def __init__(self, REST_req_args):
        self.req_args = REST_req_args
        
    def build_json():
        pass
        
        

####################################
#
#   FLASK set up
#
####################################

# defaults to running on the URL http://127.0.0.1:5000/
# access using curl with, e.g., curl --url http://127.0.0.1:5000/test/  

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return "<h1>REST-based front end for querying OPUS Short-Term Disability data.</h1>" +\
           "<p>Please include a function name such as 'claims_decided', 'decision_inventory', etc., along with appropriate parameters"

@app.route('/test/', methods=['GET'])
def test():
    return "This is a test"

####################################
#
#   Functions that parse input parameters
#
####################################


# parses the input string of a REST call and does some initial error and warning checks
def parse_input_args(r):
    out = {}
    error = ''
    warning = ''
    if 'subfunction' in r.req_args:
        r.subfunction = r.req_args['subfunction']
    if 'today' in r.req_args:
        date_str = r.req_args['today']
        r.today = date_str
        if (date_str <= '2019-12-31') or (date_str >= '2020-04-25'):
            error = error + "Provided 'today' date of '" + date_str + "' is out of range from 2020-01-01 to 2020-04-24.  "

    integer_params = ['months_before', 'biz_days_before', 'biz_days_count', 'percent']
    rec_bounds = {}
    rec_bounds['months_before'] = 6
    rec_bounds['biz_days_before'] = 180
    rec_bounds['biz_days_count'] = 60
    rec_bounds['percent'] = 100
    for param in integer_params: 
        if param in r.req_args:
            try:
                val = int(r.req_args[param])
                setattr(r, param, val)
                print('have set attr ' + param + ' to value ' + str(val) + ' which has type ' + str(type(val)))
                out[param] = val
                if val > rec_bounds[param]:
                    print('inside the val > rec_bounds[param]')
                    warning = warning  + "The value '"  + str(r.req_args[param]) + "' provided for '" + param + "' should be <= " \
                                       + str(rec_bounds[param]) + ".  "
            except:
                error = error + "The value '" + str(r.req_args[param]) + "' provided for '" + param + "' is not an integer.  "

    r.error = error
    r.warning = warning

    # return out    

'''
# parses the input string of a REST call and does some initial error and warning checks
def parse_input_args1(args):
    out = {}
    error = ''
    warning = ''
    if 'subfunction' in args:
        out['subfunction'] = args['subfunction']
    if 'today' in args:
        date_str = args['today']
        out['today'] = date_str
        if (date_str <= '2019-12-31') or (date_str >= '2020-04-25'):
            error = error + "Provided 'today' date of '" + date_str + "' is out of range from 2020-01-01 to 2020-04-24.  "

    integer_params = ['months_before', 'biz_days_before', 'biz_days_count', 'percent']
    rec_bounds = {}
    rec_bounds['months_before'] = 6
    rec_bounds['biz_days_before'] = 180
    rec_bounds['biz_days_count'] = 60
    rec_bounds['percent'] = 100
    for param in integer_params: 
        if param in args:
            try:
                val = int(args[param])
                out[param] = val
                if val > rec_bounds[param]:
                   warning = warning + "The value '" + str(args[param]) + "' provided for '" + param + "' should be <= " \
                                      + str(rec_bounds[param]) + ".  "
            except:
                error = error + "The value '" + str(args[param]) + "' provided for '" + param + "' is not an integer.  "

    out['error'] = error
    out['warning'] = warning

    return out    
'''

####################################
#
#   Panel 1 "Overview" queries
#
####################################


# e.g., invoke: curl --url http://127.0.0.1:5000/claims_decided/?date=2019-11-10
@app.route('/claims_decided/', methods=['GET'])
def claims_decided():
    # print('type of request.args is: ' + str(type(request.args)))
    
    # setting r as a new Request object
    r = Request(request.args)
    parse_input_args(r)
    
    print(r.subfunction)
    
    # 'ad' for arguments_dictrionary
    # ad = parse_input_args1(request.args)  # should include arg 'today'
    subfunc_list = ['total_this_year',
                    'total_this_month',
                    'total_in_period',               # should include arg 'months_before' XOR ''biz_days_before'
                    'TAT_gt_n_in_period']            # should include arg 'biz_days_count'
    r.end_date = r.today 
    if r.subfunction == 'total_this_year':
        r.start_date = utils_general.first_day_of_year_of_date(r.today)
    elif r.subfunction == 'total_this_month':
        r.start_date = utils_general.first_day_of_month_of_date(r.today)
    elif r.subfunction in ['total_in_period', 'TAT_gt_n_in_period']:
        if hasattr(r, 'biz_days_before'):
            if hasattr(r, 'months_before'):
                r.error = r.error + "This subfunction should have a 'biz_days_before' parameter or " \
                                  + "a 'months_before' parameter, but not both,  "
            else:
                count = 1 - r.biz_days_before    # e.g., if biz_days_before is 5 the value here is -4
                r.start_date = utils_general.biz_days_offset(r.today, count)
        elif hasattr(r, 'months_before'):            
            r.start_date = utils_general.date_n_cal_months_before(r.today, r.months_before)
        else:
            r.error = r.error + "This subfunction should have a 'biz_days_before' parameter or " \
                              + "a 'months_before' parameter, but it has neither.  "
                                      
    if r.subfunction == 'TAT_gt_n_in_period':
        if not hasattr(r, 'biz_days_count'):
            r.error = r.error + "This subfunction should have a 'biz_days_count' parameter, but it does not.  "
    
    print('\nThe full value of of the Request object r is: ')
    temp = json.dumps(r.__dict__, indent=2)
    print(temp)
    
    if r.error != '':
        out = {}
        out['error'] = r.error
        if r.warning != '':
            out['warning'] = r.warning
        temp1 = json.dumps(out, indent=2)
        print('\nThere was an error; the error messages are: ')
        print(temp1)
        return(temp1)
    else:
        # return json.dumps({'status': 'got to place where we call basic_query_processing'})
        out = basic_query_processing.claims_decided_query(db, r, 'csv_and_json')
        return out


####################################
#
#   Stuff below might be deprecated
#
####################################



# e.g., invoke: curl --url http://127.0.0.1:5000/inventory/?date=2019-11-10
@app.route('/inventory/', methods=['GET'])
def inventory():
    print('\nHave entered front_end.inventory function')
    print(str(request.args))
    if 'date' in request.args:
        date = str(request.args['date'])
    else:
        date = DEFAULT_DATE_FOR_TODAY
    print('date has value: ' + date)
    list = [date]
    
    if not basic_query_processing.valid_inventory_input(['inventory'] + list, basic_query_processing.suggested_dates):
        print('=====> input data was bad')
        return json.dumps({'ERROR': 'input data was bad'})
    else:    
        output = basic_query_processing.inventory_query(db, list, 'json')
        return output

# e.g., invoke: 
#   curl --url http://127.0.0.1:5000/inv-agg/?'date=2019-11-20&values=diagnosis,claims_analyst'
@app.route('/inv-agg/', methods=['GET'])
def inv_agg():
    print('\nHave entered front_end.inv-agg function')
    print(str(request.args))
    if 'date' in request.args:
        date = str(request.args['date'])
    else:
        date = DEFAULT_DATE_FOR_TODAY
    print('date has value: ' + date)
    list = [date]
    if 'values' in request.args:
        values_string = str(request.args['values'])
        print(values_string)
        values_list = values_string.split(sep=',')
        print(str(values_list))
        list = list + values_list
        print(str(list))
    if not basic_query_processing.valid_inv_agg_input(['inventory'] + list, 
                                            basic_query_processing.suggested_dates,
                                            basic_query_processing.permitted_columns):
        print('=====> input data was bad')
        return json.dumps({'ERROR': 'input data was bad'})
    else:            
        output = basic_query_processing.inv_agg_query(db, list, 'json')
        return output


# e.g., invoke
#   curl --url http://127.0.0.1:5000/agg-TAT/?'date1=2019-11-20&date2=2019-11-24&values=diagnosis,claims_analyst'
@app.route('/agg-TAT/', methods=['GET'])
def agg_TAT():
    print('\nHave entered front_end.agg_TAT function')
    print(str(request.args))
    date_list = []
    if 'date1' in request.args:
        date1 = str(request.args['date1'])
        date_list.append(date1)
    else:
        print('ERROR: There was no date1 value')
    if 'date2' in request.args:
        date2 = str(request.args['date2'])
        print('date2 has value: ' + date2)
        date_list.append(date1)
    else:
        print('ERROR: There was no date2 value')
    if 'values' in request.args:
        values_string = str(request.args['values'])
        print(values_string)
        values_list = values_string.split(sep=',')
        print(str(values_list))
        list = date_list + values_list
        print(str(list))
    if not basic_query_processing.valid_agg_input(['agg-TAT'] + list, 
                                            basic_query_processing.suggested_dates,
                                            basic_query_processing.permitted_columns):
        print('=====> input data was bad')
        return json.dumps({'ERROR': 'input data was bad'})
    else:            
        output = basic_query_processing.agg_query(db, list, 'TAT', 'json')
        return output



# e.g., invoke
#   curl --url http://127.0.0.1:5000/agg-gen/?'date1=2019-11-20&date2=2019-11-24&values=diagnosis,claims_analyst'
#   or curl --url http://127.0.0.1:5000/agg-gen/?'date1=2019-11-20&date2=2019-11-28&values=claims_analyst,igo_nigo'
@app.route('/agg-gen/', methods=['GET'])
def agg_gen():
    print('\nHave entered front_end.agg_gen function')
    print(str(request.args))
    date_list = []
    if 'date1' in request.args:
        date1 = str(request.args['date1'])
        date_list.append(date1)
    else:
        print('ERROR: There was no date1 value')
    if 'date2' in request.args:
        date2 = str(request.args['date2'])
        print('date2 has value: ' + date2)
        date_list.append(date1)
    else:
        print('ERROR: There was no date2 value')
    if 'values' in request.args:
        values_string = str(request.args['values'])
        print(values_string)
        values_list = values_string.split(sep=',')
        print(str(values_list))
        list = date_list + values_list
        print(str(list))
    if not basic_query_processing.valid_agg_input(['agg-TAT'] + list, 
                                            basic_query_processing.suggested_dates,
                                            basic_query_processing.permitted_columns):
        print('=====> input data was bad')
        return json.dumps({'ERROR': 'input data was bad'})
    else:            
        output = basic_query_processing.agg_query(db, list, 'gen', 'json')
        return output









####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':

    
    db = utils_postgres.connect_postgres()
    # lanuch the flask-based web server
    app.run()


