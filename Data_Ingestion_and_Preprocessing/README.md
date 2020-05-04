

This directory holds scripts for ingesting the "raw" simulated claims data from ../Raw_and_Derived_Data, and pre-processing that data into various sythesized output data sets.
The data sets are written as both .csv's and as tables in a local PostGres

At present the focus of these scripts is on building a table "claims_with_durations", which holds about 42K synthesized claims.  Each claim holds data about the decisioning phase and the pay-out phase of the claim.  This table is built from a seed table with about 5K claims that Ramesh provided.  The processing steps are as follows:

1. import_OPUS_data_to_buld__claims_raw_biz.py: Imports the "raw" data from ../Raw_and_Derived_data/, and does some cleaning.  (This does not bring in all columns).  This creates the Postgres table "claims_raw_biz".

2. add_columns_about_decisioning_to_build__claims_extended.py: This adds several columns related to the decisioning processs, to create the table "claims_extended".  This script augments the claims from the "raw" data by adding in a manual stage for "intake_decisioning", which involves an intake_analyst deciding whether the claim is IGO or NIGO.

3. replicate_claims_extended_to_build__claims_replicated.py: This takes the output data set of previous script (about 5K records with received_date between Oct 30, 2019, and November 22, 2019) and replicates it 8 times, to create about 42K claims (with received_date between Oct 30, 2019, and April 24, 2020).

4. augment_with_durations_to_build__claims_with_durations.py: This takes the output of previous script and adds numerous columns relating to the pay-out phase of the claims.


The shell script "ingest_claims_csv_and_create_postgres_tables.sh" can be used to invoke these in order from inside the current directory (../OPUS-insights_engine/Data_Ingestion_and_Preprocessing).  (This script includes the command "source activate base" which is needed if you are using the Anaconda/Spyder IDE for creating the python scripts.  This command might need to be commented out in some python environments.)

We will probably add some more scripts here to create data sets that correspond to (simulated)  data that would be coming from the various systems-of-record in our hypothetical insurance company.  This may involve 2 to 4 different tables, corresponding to the 2 to 4 hypothetical systems-of-record in the insurancy company.  Those tables will include a column for "date_data_created", that is, for the date on which the particular data record became available from the system-of-record.