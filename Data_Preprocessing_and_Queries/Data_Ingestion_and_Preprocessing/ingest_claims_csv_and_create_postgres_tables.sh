#!/bin/sh

# use this to switch the running of python3
# from /usr/bin/python3 to /Applications/Anaconda/anaconda3/bin/python3
source activate base

echo '\nLaunching the script to import raw OPUS data into postgres\n'
python3 import_OPUS_data_to_create__claims_raw_biz.py

echo '\n\n##############################################'
echo '\nLanching the script to add several columns about the decisioning phase\n'

python3 add_columns_about_decisioning_to_build__claims_extended.py

echo '\n\n##############################################'
echo '\nLanching the script to replicate the 7K claims to create about 30K claims\n'

python3 replicate_claims_extended_to_build__claims_replicated.py


echo '\n\n##############################################'
echo '\nLanching the script that adds columns relating to durations\n'

python3 augment_with_durations_to_build__claims_with_durations.py





