#!/bin/bash

kinit ivukotic@CERN.CH -k -t /afs/cern.ch/user/i/ivukotic/ivukotic.keytab

cd /afs/cern.ch/user/i/ivukotic/xAOD-analytics/import
DateToProcess=$1

echo "deleting index if it already existed. "
curl -XDELETE http://cl-analytics.mwt2.org:9200/xaod_$1

echo "Indexing...  "${DateToProcess}
pig -4 log4j.properties -f JobIndexer.pig -param INPD=${DateToProcess}
echo "pig code finished."

echo "Done. Starting the other indexer..."
rm -f heatmap.csv
hdfs dfs -getmerge heatmap.csv heatmap.csv
python indexer.py ${DateToProcess}
echo "upload finished."
