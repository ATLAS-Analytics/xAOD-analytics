#!/bin/bash

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

cd /afs/cern.ch/user/i/ivukotic/xAOD-analytics/pig
DateToProcess=$(date +%Y-%m-%d -d "-1day")
#DateToProcess=2015-11-25
echo "Indexing...  "${DateToProcess}
pig -4 log4j.properties -f JobIndexer.pig -param INPD=${DateToProcess}

echo "Done. Starting the other indexer..."
pig -4 log4j.properties -f accessesIndexer.pig -param INPD=${DateToProcess}
echo "pig code finished."
rm -f heatmap.csv
hdfs dfs -getmerge heatmap.csv heatmap.csv
python indexer.py ${DateToProcess}
echo "upload finished."