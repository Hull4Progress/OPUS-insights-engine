This top-level directory will hold subdirectories for the major subsystems of the OPUS Insights Engine.

There are currently 2 subdirectories:

- Documents: This holds selected documents about the project, primarly in .docx and .pptx format.

- Data_Preprocessing_and_Queries: Holds python/SQL scripts for 2 main activities:
  - Preprocessing of synthetic data to produce a synthetic example of data in the "OPUS target schema"
  - Supporting REST APIs that invoke SQL queries against the data held in the target schema

We anticipate that two additional subdirectories will be added by Sunder, which will hold the codebases
for the
- Data Ingestion and Processing Pipeline
- UI

Also, code will be developed for producing synthetic data corresponding to the data sets available in
an insurance company's systems of record.  
