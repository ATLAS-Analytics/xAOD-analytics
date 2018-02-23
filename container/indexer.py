#!/usr/bin/env python

import sys
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers

es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}])
es.cluster.health(wait_for_status='yellow', request_timeout=10)

df = open('heatmap.csv')
rws = df.readlines()
print len(rws)

ind = "xaod_accesses_" + sys.argv[1]

aLotOfData = []
for l in rws:
    els = l.split(',', 4)
    acc = els[4].replace('"', '')
    acc = acc.split('},{')
    accB = acc[0][1:]
    branches = accB.split(',')
    for b in branches:
        data = {'_index': ind}
        data['FileType'] = els[0]
        data['Grid'] = int(els[1])
        data['nJobs'] = int(els[2])
        data['timestamp'] = long(els[3])
        kv = b.split("=")
        if len(kv) < 2:
            # print "empty:", kv, l
            continue
        data['type'] = 'BranchAccesses'
        data['_type'] = 'doc'
        try:
            data['branch'] = kv[0].replace(".", ":").lstrip()
            data['jobs'] = int(kv[1])
        except:
            print kv
            print l
            break
        aLotOfData.append(data)
    accC = acc[1].rstrip('}\n')
    containers = accC.split(',')
    for b in branches:
        data = {'_index': ind}
        data['FileType'] = els[0]
        data['Grid'] = int(els[1])
        data['nJobs'] = int(els[2])
        data['timestamp'] = long(els[3])
        kv = b.split("=")
        if len(kv) < 2:
            # print "empty:", kv
            # print l
            continue
        data['type'] = 'ContainerAccesses'
        data['_type'] = 'doc'
        try:
            data['container'] = kv[0].strip().rstrip(".").replace(".", ":")
            data['jobs'] = int(kv[1])
        except:
            print kv
            print l
            break
        aLotOfData.append(data)


try:
    res = helpers.bulk(es, aLotOfData, raise_on_exception=True)
    print "inserted:", res[0], '\tErrors:', res[1]
    aLotOfData = []
except es_exceptions.ConnectionError as e:
    print 'Connection Error >>> ', e
except es_exceptions.TransportError as e:
    print 'Transport Error >>> ', e
except helpers.BulkIndexError as e:
    print 'Bulk indexing Error >>> ', e[0]
    for i in e[1]:
        print i
except:
    print 'Something seriously wrong happened. '
