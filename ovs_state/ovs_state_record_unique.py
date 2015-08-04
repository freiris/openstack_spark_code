"""
Expand the ovsdb snapshot into uuid-level record, and remove the adjacent duplicates, 
finally parse the record as json format. 
"""


from __future__ import print_function
import sys
import ast
import json
from pyspark import SparkContext, SparkConf



def expand(ovsState): # database snapshot
	timestamp = ovsState['timestamp']
	IP = ovsState['IP']
	res = []
	for key in ovsState:
		if key != 'timestamp' and key != 'IP':
			table = ovsState[key]
			headings = table['headings']
			data = table['data']
			dictData = [None] * len(data)

			for i, record in enumerate(data):
				dictRecord = {'timestamp': timestamp, 'IP': IP} # extra info
				for key, val in zip(headings, record):
					dictRecord[key] = val
				dictData[i] = dictRecord
			res += dictData
	return res		


def unique(records):
	referRecord = records[0] # records has at least 1 element after reduceByKey()
	keys = [key for key in referRecord.keys() if key not in ['statistics', 'IP', 'timestamp']]#ignore the two extra keys we argumented
	res = []
	for record in records:
		for key in keys:
			if record[key] != referRecord[key]:
				res.append(referRecord)
				referRecord = record
				break
	else:
		res.append(referRecord)

	return res		


def parse(data):
	if isinstance(data, dict):
		for key in data:
			data[key] = parse(data[key])
		return data
	elif isinstance(data, list):
		if len(data) == 2: # [datatype, values]
			if data[0] == 'map':
				data[1] = parse(dict(data[1]))
				return data[1]
			elif data[0] == 'set':
				for i, ele in enumerate(data[1]):
					data[1][i] = parse(ele)
				return data[1] 
			elif data[0] == 'uuid':
				return data[1]
	else:
		return data




if __name__ == "__main__":
    	if len(sys.argv) != 3:
        	print("Usage: ovs_state_record_urecord_unique.py <pathIn> <pathOut>")
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
	ovsExpandRDD = (ovsRDD
#			.map(lambda line: ast.literal_eval(line.strip('\n')))
			.map(ast.literal_eval)
			.flatMap(expand)
			)

	samples = ovsExpandRDD.take(10)
	# assume _uuid is practically non-collision across ovsdb, otherwise we group by IP at first (We assured it with a data set)
	ovsSortRDD = (ovsExpandRDD
			.map(lambda record: (record['_uuid'][1], [record])) # record['_uuid'] is list (i.e, [u'uuid', uuidValue]) and unhashale 
			.reduceByKey(lambda x, y: x + y)
			.mapValues(lambda records: sorted(records, key = (lambda record: record["timestamp"]), reverse = False))
			)
	
	ovsUniqRDD = ovsSortRDD.mapValues(unique)

#	ovsParseRDD = ovsUniqRDD.mapValues(lambda records: [parse(record) for record in records])
	ovsParseRDD = ovsUniqRDD.mapValues(lambda records: map(lambda record: parse(record), records))
		
	ovsParseRDD.saveAsTextFile(pathOut)	

	sc.stop()
