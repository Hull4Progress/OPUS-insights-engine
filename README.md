# OPUS-insights-engine

This repository holds a mix of python3, SQL, and shell code for manipulating insurance 
claims data according to the OPUS framework.  The code assumes that
- appropriate raw data about claims is available in a .csv file
- an instance of postgres is intalled on your local machine

Key modules currently include (this was written 2020-04-15 and may now be out of date)
- constants_used_for_data_manip.py: holds various constants, 
such as password for local postgres installations, location of data output directory, etc
- import_OPUS_data.py: used to import "raw" data provided by Ramesh that includes about 
approximately 5K claims (with receive dates between 2019-11-01 to 2019-11-15); builds the table claims_raw
- initial_claims_processing__columns_and_days.py: adds many columns to the claims_raw table,
and also synthesizes claims for many new days (with receive dates from roughly 2019-08-01 to 2019-10-31) to create 
the claims_extended table. (For now, this script also builds a table claims_agg, which is a fine-grained
group-by aggregation table.  However, when we incorporate dates into our group-by fields
this table claims_agg probably doesn't make sense any more.)
- build_cube.py: script that will build a requested group-by aggregation table and write
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
