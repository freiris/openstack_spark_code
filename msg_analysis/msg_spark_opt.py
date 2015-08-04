"""
Extract message based on the uuid from libvirt state or ovs state,
and only parse the selected messages into json format.
"""
from __future__ import print_function
import re
import sys
import json
from pyspark import SparkContext, SparkConf


def parseMsg(data):
	if isinstance(data, unicode) or isinstance(data, str): # unicode/string
                try: # test whether a JSON string or not
                        newData = json.loads(data)# newData can be dict, list, unicode...
                except: # ordinary string, not in JSON format
                        return data
                else: # valid JSON string
                        return parseMsg(newData)
        elif isinstance(data, dict): # dictionary
                for key, value in data.items():
                        data[key] = parseMsg(value)
                return data
        elif isinstance(data, list): # list
                for i in range(len(data)):
                        data[i] = parseMsg(data[i])
                return data
        else: # other primary types, i.e. int,long,float,True, False, None 
                return data

def commonUuid(msgline):
        uuidPtn = r'[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}'
        uuids = set(re.findall(uuidPtn, msgline))
        commUuids = uuids.intersection(broadcastUuids.value)
        return (commUuids, msgline)

def pairMsgUuid(uuidMsg):
#       "input: uuidMsg is ([uuid], parsedMsg)"
        res = [None] * len(uuidMsg[0])
#        i = 0
        for i, uuid in enumerate(uuidMsg[0]):
                res[i] = (uuid, uuidMsg[1])
#                i += 1
        return res

	

if __name__ == "__main__":
    	print(sys.argv)
    	if len(sys.argv) != 4:
        	print("Usage: msg_spark.py <msgPathIn> <msgPathOut> <uuidPathIn>")
        	exit(-1)
    	
	conf = SparkConf()
    	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	conf.set("spark.speculation", "true")
    	conf.setAppName("PythonMsg")
    	sc = SparkContext(conf=conf)
    	
	msgPathIn = sys.argv[1]
    	msgPathOut = sys.argv[2] 
    	uuidPathIn = sys.argv[3]
	print('msgPathIn = %s, msgPathOut = %s, uuidPathIn = %s' % (msgPathIn, msgPathOut, uuidPathIn))

	uuids = sc.textFile(uuidPathIn).collect()
	broadcastUuids = sc.broadcast(uuids)
#	uuidPtn = r'[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}'	
#	broadcastUuidPtn = sc.broadcast(uuidPtn)

	msgRDD = sc.textFile(msgPathIn)
	msgUuidRDD = (msgRDD
		      .map(lambda x: x.strip(',\n')) # remove the ',\n'
		      .map(commonUuid)
		      .filter(lambda (commUuids, msgline): len(commUuids) > 0)
		      .mapValues(parseMsg)
		      .flatMap(pairMsgUuid)
		     )
	msgUuidGroupRDD = (msgUuidRDD
			    .mapValues(lambda msg: [msg])
			    .reduceByKey(lambda p, q: p + q)) # avoid groupByKey()
	
#	msgUuidGroupRDD = msgUuidRDD.groupByKey()		
	msgUuidGroupSortedRDD = (msgUuidGroupRDD
				.mapValues(lambda msgs: sorted(msgs, key = (lambda msg: msg['timestamp']), reverse = False))
#				.cache()
				)

	msgUuidGroupSortedRDD.saveAsTextFile(msgPathOut)
#	msgUuidGroupSortedRDD.mapValues(lambda msgs: [msg['timestamp'] for msg in msgs]).foreach(print) # debug purpose

	sc.stop()
