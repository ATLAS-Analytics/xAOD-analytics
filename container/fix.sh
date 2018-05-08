#!/bin/bash
 
Ind="2018-01"
for i in $(seq -f "%02g" 1 31)
do

DateToProcess="$Ind-$i"
echo "Job Indexing...  "${DateToProcess}

pig -4 log4j.properties -f JobIndexer.pig -param INPD=${DateToProcess}
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with pig indexer. Exiting."
    exit $rc
fi
echo "pig code finished."

echo "Done. Starting the other indexer..."
rm -f heatmap.csv
hdfs dfs -getmerge heatmap.csv heatmap.csv
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with getmerge. Exiting."
    exit $rc
fi
python indexer.py ${IND}
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with python indexer. Exiting."
    exit $rc
fi
echo "upload finished."
   
done