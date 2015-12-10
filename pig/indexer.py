#!/usr/bin/env python

from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers


print "make sure we are connected right..."
import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])

df = open('heatmapEvents.csv')
rws=df.readlines()[:100]
print len(rws)

# d = datetime.now()
# ind="xAOD_test-"+str(d.year)+"."+str(d.month)+"."+str(d.day)
ind="xaod_test-2015.12.08"


aLotOfData=[]
for l in rws:
    els=l.split(',',3)
    acc=els[3].replace('"','')
    acc=acc.split('},{')
    accB=acc[0][2:]
    branches=accB.split(',')
    for b in branches:
        data = {
            '_index': ind
            }
        data['TaskID']=int(els[0])
        data['FileType']=els[1]
        data['nJobs']=int(els[2])
        kv=b.split("=")
        if len(kv)<2: 
            #print "empty:", kv
            #print l
            continue
        data['_type']='BranchAccesses'
        try:
            data['branch']=kv[0].replace(".",":").lstrip()
            data['events']=int(kv[1])
        except:
            print kv
            print l
            break
        aLotOfData.append(data)
    accC=acc[1].rstrip('}\n')
    containers=accC.split(',')
    for b in branches:
        kv=b.split("=")
        if len(kv)<2: 
            #print "empty:", kv
            #print l
            continue
        data['_type']='ContainerAccesses'
        try:
            data['branch']=kv[0].rstrip(".").replace(".",":")
            data['events']=int(kv[1])
        except:
            print kv
            print l
            break
        aLotOfData.append(data)

try:
    res = helpers.bulk(es, aLotOfData, raise_on_exception=False)
except:
    print 'Something seriously wrong happened. '