#!/usr/bin/env python

"""
NetGrph ACLs Import Routines
"""

import csv
import clustering
import argparse

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
from pprint import pprint
import string
import random

tempDict = {}

data = []


# toAdd = ["product_name",
#          "crpc",
#          "count",
#          "measure",
#          "price",
#          "contract_name",
#          "region",
#          "provider",
#          "inn",
#          "date",
#          "cluster_labels",
# ]

toAdd = ["product_name",
         "category"
]

header = ""
for word in toAdd:
    header += word + ";"

header = header[:-1]


def uploadData(data):
    es = Elasticsearch([''],
                       http_auth=('elastic', ''))
    helpers.bulk(es, data)

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def getEsElem(id, index, source):
    ourDictionary = {}

    ourDictionary["_index"] = index[:-4]
    ourDictionary["_type"] = "_doc"
    ourDictionary["_id"] = id_generator()
    ourDictionary["_source"] = source

    return ourDictionary

def convert_file(filename):
    with open(filename, "r") as infile:
        reader = list(csv.reader(infile))
        reader.insert(0, [header])
    with open("temp.csv", "w") as outfile:
        writer = csv.writer(outfile)
        for line in reader:
            writer.writerow(line)


def mapping_proc(data, filename):
    with open("temp.csv", "r") as infile:
        input_file = csv.DictReader(infile, delimiter=';', quoting=csv.QUOTE_NONE)
        for index, row in enumerate(input_file):
            curr_row = (dict(row))
            # if curr_row["cluster_labels"][-1:] == '"':
            #     curr_row["cluster_labels"] = (curr_row["cluster_labels"][:-1]) # чистка от лишней ковычки с конца лейбла
            # if curr_row["cluster_labels"].isdigit() or curr_row["cluster_labels"] == "-1":
            #     curr_row["cluster_labels"] = ((curr_row["cluster_labels"]))
            # else:
            #     curr_row["cluster_labels"] = "-2"
            # curr_row["cluster_labels"] = int(curr_row["cluster_labels"])
            data.append(getEsElem(index, filename, curr_row))
    return data

def import_good_clusters(filename):
    print("Mapping to ESjson")
    convert_file(filename)
    mapping_proc(data, filename)
    print("Mapping complete")
    print("Upload data to ES server")
    # ccounter = 0
    # for i in range(len(data)):
    #     if data[i]["_source"]['cluster_labels'] == -2:
    #         ccounter += 1
    # print(ccounter)
    uploadData(data)
    print("Uploading complete and available as index: {}".format(filename[:-4]))

def getArgs():
    parser = argparse.ArgumentParser(description='Clustering goods')
    parser.add_argument("-c", metavar='csv_test_file_path', help='CSV file from SPARK-marketing directory',
                        type=str)
    return parser.parse_args()


args = getArgs()
# clustering.cluster(args.c, 'out_' + args.c)
import_good_clusters('out_' + args.c)


