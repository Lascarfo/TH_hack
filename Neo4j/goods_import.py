#!/usr/bin/env python

"""
NetGrph ACLs Import Routines
"""
#import logging
#import nglib
import csv
import py2neo
from py2neo import Graph
#logger = logging.getLogger(__name__)
import time
import datetime
graph = Graph("http://localhost:7474/db/data", user='neo4j', password='123456')
#print(graph.evaluate('MERGE (n) RETURN n'))

print('Succesful connect')
def get_time(hours=None):
    """Get current time, optionally time shifted by hours"""

    if hours:
        timeShifted = datetime.datetime.now() - datetime.timedelta(hours=hours)
        return str(timeShifted)

    time = str(datetime.datetime.now())
    return time

def importCSV(fileName):
    """Imports a file with DictReader"""

    f = open(fileName)
    db = csv.reader(f, delimiter=';')
    return db

def import_good_clusters(fileName):
    """Import ACL CSV File"""

    #logger.info("Importing Firewalls from " + fileName)

    fwdb = importCSV(fileName)
    i = 0
    output_list = []
    for row in fwdb:
        row.append(i)
        i += 1
        output_list.append(row)

    import_int_acls(output_list)


def import_int_acls(goods_list):
    """Import ACL from acl File"""

    time = get_time()

    clusters_idx = set()

    for good in goods_list:

        #  DB values
        # 0 - product_name
        # 1 - crpc(common russian product classifier)
        # 2 - count
        # 3 - measure
        # 4 - price
        # 5 - contract_name
        # 6 - region
        # 7 - provider
        # 8 - inn
        # 9 - date
        # 10 - cluster_labels

        product_name = good[0]
        crpc = good[1]
        count = good[2]
        measure = good[3]
        price = good[5]
        contract_name = good[6]
        region = good[7]
        provider = good[8]
        inn = good[9]
        date = good[10]
        cluster = good[11]

        cluster = good[10]  # 0 class
        data = good[0]  # 1 data
        idx = good[12]
        if cluster == -1:
            continue
        print(cluster, ' ', data, ' ', idx)

        if cluster not in clusters_idx:
            clusters_idx.add(cluster)
            print('Cluster added:', cluster)
        results = graph.run(
            'MERGE (good:GOOD '
            + '{'
            + 'cluster:{cluster}, '
            + 'data:{data}, '
            + 'idx:{idx} '
            + '}) RETURN good',
            cluster=cluster,
            data=data,
            idx=idx)
    print('Products imported succesful')

    for cluster_idx in clusters_idx:
        results = graph.run(
            'MERGE (cluster:CLUSTER'
            + '{'
            + 'cluster:{cluster_idx}'
            + '}) RETURN cluster',
            cluster_idx=cluster_idx)
    print('Clusters imported succesful')

    for good_idx in range(0, len(goods_list)):
        results = graph.run(
            'MATCH (good1:GOOD {idx:{idx1}}), (cluster:CLUSTER {cluster:{cluster_idx}})'
            + 'CREATE UNIQUE (good1)<-[:Similiar]-(cluster)',
            idx1=goods_list[good_idx][12], cluster_idx=goods_list[good_idx][10])
    print('Connections imported succesful')

    # for good_idx in range(0, len(goods_list)):
    #     for good_idx2 in range(good_idx+1, len(goods_list)):
    #         if goods_list[good_idx][0] == goods_list[good_idx2][0]:
    #             results = graph.run(
    #                 'MATCH (good1:GOOD {idx:{idx1}}), (good2:GOOD {idx:{idx2}})'
    #                 + 'CREATE UNIQUE (good1)-[:Similiar]->(good2)',
    #                 idx1=goods_list[good_idx][2], idx2=goods_list[good_idx2][2])
    #             # print('MATCH (good1:GOOD {idx:{idx1}}), (good2:GOOD {idx:{idx2}})'
    #             #     + 'CREATE UNIQUE (good1)-[:Similiar]->(good2)',
    #             #     idx1=goods_list[good_idx][2], idx2=goods_list[good_idx2][2])


import_good_clusters('outt_fuel.csv')
