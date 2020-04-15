#!/bin/sh

# use this to switch the running of python3
# from /usr/bin/python3 to /Applications/Anaconda/anaconda3/bin/python3
source activate base

echo '\nLaunching the script to import raw OPUS data into postgres\n'
python3 import_OPUS_data.py

echo '\n\n##############################################'
echo '\nLanching the script to synthesize addigional claims and add several columns to them\n'

python3 initial_claims_processing__columns_and_days.py

# echo '\n\n##############################################'
# echo '\nLanching the script that explores data cube operations\n'

# python3 cube_explorations.py





