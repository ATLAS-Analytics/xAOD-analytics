#!/bin/bash

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

cd /afs/cern.ch/user/i/ivukotic/xAOD-analytics/import
DateToProcess=$(date +%Y-%m-%d -d "-1day")
IND=$(date +%Y.%m -d "-1day")
echo "Indexing...  "${DateToProcess}
pig -4 log4j.properties -f JobIndexer.pig -param INPD=${DateToProcess} -param INDE=${IND}
echo "pig code finished."

echo "Done. Starting the other indexer..."
rm -f heatmap.csv
hdfs dfs -getmerge heatmap.csv heatmap.csv
python indexer.py ${IND}
echo "upload finished."
