


This GitHub repository holds code related to the OPUS business insights initiative.

Currently there are 4 directories here, namely:

- Constants: this currently holds a python script with constants that are used elsewhere in the python code.  We might add a similar file for constants used in Java code.

- Raw_and_Derived_Data: this holds files with raw data, primarily a file with about 5K synthetic claims for short-term disability insurance.  It also holds selected output data files. 

- Data_Ingestion_and_Preprocessing: holds a family of python+sql scripts that ingest the synthetic data and augments it in various ways.  In general the derived files are written into a local Postgres and also into .csv files for easy inspection and sharing.  (The .csv files are typically written to a directory that is outside of the GitHub directory, according to a value in the ../Constants python script.)

- Producing_Requested_Outputs: This directory holds the REST APIs for the various output queries that the UI will be invoking.  It also holds python+sql scripts for invoking those queries against the local Postgres.


