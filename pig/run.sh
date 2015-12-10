#!/bin/bash

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

cd /afs/cern.ch/user/i/ivukotic/xAOD-analytics/pig
DateToProcess=$(date +%Y-%m-%d)
DateToProcess=2015-11-25
echo "Indexing...  "${DateToProcess}
pig -4 log4j.properties -f JobIndexer.pig -param INPD=${DateToProcess}
