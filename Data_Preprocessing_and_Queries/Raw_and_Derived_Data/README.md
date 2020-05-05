
This directory holds a small number of data sets.

We have included here one "output" data set, namely
- 2020-05-04--19-19__claims_with_durations.csv
This was produced using scripts in ../Data_Ingestion_and_Preprocessing/.  We include this file here because some of the values inside are generated using random numbers.  The idea is to build the rest of the demo based on this fixed output file (and/or projections of it).

We may tweak the scripts that produce this data file (e.g., to modify the average behaviors in terms of time for different decisioning stages.

We have also included the small table
- TARGET-SCHEMA-with-INPUT-TABLE-MARKEUP--v01.xlsx
This is the first 100 or so rows of the clamis_with_durations table, but with an added row in the second position.
That row holds the names of (hypothetical) source tables where the different columns would be coming from.
This markup can be used as the basis for creating some synthetic versions
of data coming from the source tables, including arrivals on different dates.



"Raw" data sets used as input include:
- 2020-04-13a--Total - 5386.xlsx: this is a spreadsheet with simulated data -- about 5K claims -- created by Ramesh
- 2020-04-13b--Total - 5386.csv: a slightly cleaned version of the .xlsx.  Most of the "cleaning" is done in the python routine found in ../Data_Ingestion_and_Preprocessing/import_OPUS_data_to_build__clams_raw_biz.py
- Diagnosis_Duration_Parameters_v01.xlsx: this is a spreadsheet created by Ramesh, that holds proposed data relating to the target durations for the pay-out period for the different diagnoses.  It also includes the recommended number of "touches" by analysts, and the expected durations of fewer touches are made.
- Diagnosis_Duration_Parameters_v01.csv: This is a re-working of Ramesh's file (different format).  This is used in ../Data_Ingestion_and_Preprocessing/augment_with_durations_to_build__claims_with_durations.py
- Claims_Analyst_Parameters_v01.csv: This file holds data about the degree to which the claims analysts comply with the recommended number of "touches" during the pay-out period.  This is used in ../Data_Ingestion_and_Preprocessing/augment_with_durations_to_build__claims_with_durations.py.

