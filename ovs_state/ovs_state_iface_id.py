"""
Take record['external_ids']['iface-id'] as key instead of record['_uuid'] 
"""

from __future__ import print_function
import sys
import ast
import json
from pyspark import SparkContext, SparkConf

def hasIfaceId(records):
	for record in records:
		if ('external_ids' in record) and ('iface-id' in record['external_ids']):
			return True
	return False


if __name__ == "__main__":
    	if len(sys.argv) != 3:
        	print("Usage: ovs_state_iface_id.py <pathIn> <pathOut>")
        	exit(-1)

	conf = SparkConf()
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.speculation", "true")
    	conf.setAppName("PythonOvsState")
    	sc = SparkContext(conf=conf)

    	pathIn = sys.argv[1]
    	pathOut = sys.argv[2]

    	print('pathIn = %s, pathOut = %s' % (pathIn, pathOut))

	ovsRDD = sc.textFile(pathIn)
	ovsIfaceIdRDD = (ovsRDD
#			.map(lambda line: ast.literal_eval(line.strip('\n')))
			.map(ast.literal_eval)
			.filter(lambda (uuid, records): hasIfaceId(records))
			.map(lambda (uuid, records): (records[0]['external_ids']['iface-id'], records)) # take iface-id as key
			# we assure that every record has a non-empty iface-id and all the iface-ids are identical for each uuid,
			# in other words, there is an one-to-one mapping between iface-id an uuid (record['_uuid'])
			# _uuid is an internal ID within ovsdb, while iface-id can be recognized by openstack, so we swith the key
			)

	ovsIfaceIdRDD.saveAsTextFile(pathOut)
	sc.stop()
