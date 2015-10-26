#!/bin/bash

# this script will be called from a cron once a day after script importing the new data
# it runs a pig script to index the new data in ES.

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

cd /afs/cern.ch/user/i/ivukotic/xAOD-analytics/import
DateToProcess=$(date +%Y-%m-%d -d "-4day")
echo "Indexing... CL "${DateToProcess}
pig -4 log4j.properties -f HeatMapOffGridIndexing.pig -param INPD=${DateToProcess}




