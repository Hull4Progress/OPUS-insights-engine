# OPUS-insights-engine

This README.md file may be out of date.

This GitHub repository holds code related to the OPUS business insights initiative.

Currently there are 2 directories here, namely:
- Raw_Data: holds files with raw data, primarily a file with about 5K synthetic claims for short-term disability insurance
- Data_Processing: holds a family of python+sql scripts that ingest the synthetic data, augment it in various ways, and write the output into a local Postgres database.  This directory also holds some python+sql that can be used for dynamic queries against the Postgres, to see various drill downs and group-by aggregations of the claims data.
