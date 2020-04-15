#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 17:03:10 2019

@author: rick and elisa
"""

# This file holds constants used in the routines for assembling measurement-treatment-measurement TRIPLES


POSTGRES_SIGN_IN_CREDENTIALS_FOR_PSYCOPG2 = "dbname='opus' user='rick' host='localhost' password='postgres' port='5432' "
# POSTGRES_SIGN_IN_CREDENTIALS = "dbname='mimic' user='postgres' host='localhost' password='elisas96' port='5432' "


DB_ADDRESS_FOR_SQLALCHEMY = "postgresql://rick:postgres@localhost:5432/opus"


# location of synthetic claims data created by Ramesh
RAMESH_CLAIMS_CSV = "../OPUS_DATA/2020-01-18--Opus--Ramesh-example-data.csv"

OPUS_DATA_OUTPUTS_DIR = "../OPUS_DATA/DATA_OUTPUTS/"


'''
# used in build_inputevent_consec_clusters_table.py
ITER_GAP = 900 # 15 minutes)

# used in build_mv_drug_names_and_gsns.py
BUILD_TRIPLES_WORKING_FILES_DIR = "./BUILD_TRIPLES_WORKING_FILES/"

# holds the csv files from AEOLUS and drug_cleaned

# These are too big for GitHub, so we put them just outside and next to the
#    evidence-based-treatment directory that is held in github
# These files can be found in the Google Drive for nyu.mimic.project@gmail.com
SUBSTANCE_DATA_INPUT_FILES_DIR = "../../DRUG_NAME_TO_SUBSTANCE_MAPPING_DATA/"
AEOLUS_DATA_INPUT_FILES_DIR = "../../DRUG_NAMES_FROM_AEOLUS/"
'''



####################################
#
#   main starts here
#
####################################

if __name__ == '__main__':
    pass