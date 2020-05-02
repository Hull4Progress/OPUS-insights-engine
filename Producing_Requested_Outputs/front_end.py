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


import date_cubes

####################################
#
#   FLASK set up
#
####################################


app = flask.Flask(__name__)
app.config["DEBUG"] = True

# defaults to running on the URL http://127.0.0.1:5000/
# access using curl with, e.g., curl --url http://127.0.0.1:5000/test/  

@app.route('/', methods=['GET'])
def home():
    return "<h1>REST-based front end for querying OPUS Short-Term Disability data.</p>"


@app.route('/test/', methods=['GET'])
def test():
    return "This is a test"

# e.g., invoke: curl --url http://127.0.0.1:5000/inventory/?date=2019-11-10
@app.route('/inventory/', methods=['GET'])
def inventory():
    print('\nHave entered front_end.inventory function')
    print(str(request.args))
    if 'date' in request.args:
        date = str(request.args['date'])
    else:
        date = '2020-02-01'
    print('date has value: ' + date)
    list = [date]
    
    if not date_cubes.valid_inventory_input(['inventory'] + list, date_cubes.suggested_dates):
        print('=====> input data was bad')
        return json.dumps({'ERROR': 'input data was bad'})
    else:    
        output = date_cubes.inventory_query(db, list, 'json')
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
        date = '2020-02-01'
    print('date has value: ' + date)
    list = [date]
    if 'values' in request.args:
        values_string = str(request.args['values'])
        print(values_string)
        values_list = values_string.split(sep=',')
        print(str(values_list))
        list = list + values_list
        print(str(list))
    if not date_cubes.valid_inv_agg_input(['inventory'] + list, 
                                            date_cubes.suggested_dates,
                                            date_cubes.permitted_columns):
        print('=====> input data was bad')
        return json.dumps({'ERROR': 'input data was bad'})
    else:            
        output = date_cubes.inv_agg_query(db, list, 'json')
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
    if not date_cubes.valid_agg_input(['agg-TAT'] + list, 
                                            date_cubes.suggested_dates,
                                            date_cubes.permitted_columns):
        print('=====> input data was bad')
        return json.dumps({'ERROR': 'input data was bad'})
    else:            
        output = date_cubes.agg_query(db, list, 'TAT', 'json')
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
    if not date_cubes.valid_agg_input(['agg-TAT'] + list, 
                                            date_cubes.suggested_dates,
                                            date_cubes.permitted_columns):
        print('=====> input data was bad')
        return json.dumps({'ERROR': 'input data was bad'})
    else:            
        output = date_cubes.agg_query(db, list, 'gen', 'json')
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


