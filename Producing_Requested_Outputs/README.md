# OPUS-insights-engine

This README.md is a work in progress and does not attempt to be comprehensive at this point.
Also, the underlying code is evolving and this README.md may be out-of-date.



This repository holds a mix of python3, SQL, and shell code for manipulating insurance 
claims data according to the OPUS framework.  The code assumes that
- appropriate raw data about claims is available in a .csv file
- appropriate raw data about some auxiliary info is available in .csv files (this currently includes claims_analyst_parameters.csv and diagnosis_duration_parameters.csv)
- an instance of postgres is intalled on your local machine

Key modules currently include (this was written 2020-04-15 and may now be out of date)
- constants_used_for_data_manip.py: holds various constants, 
such as password for local postgres installations, location of data output directory, etc

- import_OPUS_data.py: used to import "raw" data provided by Ramesh that includes about 
approximately 5K claims (with receive dates between 2019-11-01 to 2019-11-15); builds the table claims_raw

- initial_claims_processing__columns_and_biz_days.py: adds several columns to the claims_raw table.  Soon it will
also synthesize claims for many new days (with receive dates from roughly 2019-11-22 to 2020-02-28) to create 
the full claims_extended table.

- augment_with_durations.py: adds columns to claims_extended that relate to the overall pay-out duration (between date claim was received and the date of "return-to-work". The column values are based on computations and weighting functions based on the contents of a file Diagnosis_Duration_Parameters_v01.csv.  This function also imports this csv into the table diagnosis_duration_parameters for use in a later function.

- build_cube.py: (not updated yet) script that will build a requested group-by aggregation table and write
that as a .csv into the DATA_OUTPUT direcory (for the exact location please see 
constants_used_for_data_manip.py). 

- files that end with "_7K.py" (deprecated): these were developed for the raw claims data set
with about 7K claims in it; we will delete this files in a while

- cube_explorations_7K.py (deprecated): based on a curious SQL query that creates, in essence, 
the union of many group-by aggregation tables, all placed into one table called 
claims_cube.  (This was developed for an earlier version  of the claims_raw data,
and has not yet been upgraded to the 5K claims dataset.)

- utils_general.py: some basic utility functions

- utils_postgres.py: some postgres-specific utilities

- run.sh: a shell script use to invoke some selected python scripts; right now the focus
is on initialization, spez., invoking import_OPUS_data.py and initial_claims_process__columns_and_days.py
