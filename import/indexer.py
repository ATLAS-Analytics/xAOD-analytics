#!/usr/bin/env python

import sys
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers


print "make sure we are connected right..."
import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])

df = open('heatmap.csv')
rws=df.readlines()
print len(rws)

ind="xaod_"+sys.argv[1]

aLotOfData=[]
for l in rws:
    els=l.split(',',4)
    acc=els[4].replace('"','')
    acc=acc.split('},{')
    accB=acc[0][1:]
    branches=accB.split(',')
    for b in branches:
        data = { '_index': ind }
        data['FileType']=els[0]
        data['Grid']=int(els[1])
        data['nJobs']=int(els[2])
        data['timestamp']=long(els[3])
        kv=b.split("=")
        if len(kv)<2: 
            #print "empty:", kv
            #print l
            continue
        data['_type']='BranchAccesses'
        try:
            data['branch']=kv[0].replace(".",":").lstrip()
            data['jobs']=int(kv[1])
        except:
            print kv
            print l
            break
        aLotOfData.append(data)
    accC=acc[1].rstrip('}\n')
    containers=accC.split(',')
    for b in branches:
        data = { '_index': ind }
        data['FileType']=els[0]
        data['Grid']=int(els[1])
        data['nJobs']=int(els[2])
        data['timestamp']=long(els[3])
        kv=b.split("=")
        if len(kv)<2: 
            #print "empty:", kv
            #print l
            continue
        data['_type']='ContainerAccesses'
        try:
            data['container']=kv[0].strip().rstrip(".").replace(".",":")
            data['jobs']=int(kv[1])
        except:
            print kv
            print l
            break
        aLotOfData.append(data)


try:
    res = helpers.bulk(es, aLotOfData, raise_on_exception=True)
    print threading.current_thread().name, "\t inserted:",res[0], '\tErrors:',res[1]
    aLotOfData=[]
except es_exceptions.ConnectionError as e:
    print 'ConnectionError ', e
except es_exceptions.TransportError as e:
    print 'TransportError ', e
except helpers.BulkIndexError as e:
    print e[0]
    for i in e[1]:
        print i
except:
    print 'Something seriously wrong happened. '
