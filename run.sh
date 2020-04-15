#!/bin/sh

# use this to switch the running of python3
# from /usr/bin/python3 to /Applications/Anaconda/anaconda3/bin/python3
source activate base

echo '\nLaunching the script to import raw OPUS data into postgres\n'
python3 import_OPUS_data.py

echo '\n\n##############################################'
echo '\nLanching the script to add column for hours_worked on each claim\n'

python3 manip_claims_with_pandas.py

echo '\n\n##############################################'
echo '\nLanching the script that explores data cube operations\n'

python3 cube_explorations.py





