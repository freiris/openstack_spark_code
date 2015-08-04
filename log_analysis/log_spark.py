from __future__ import print_function
import re
import sys
import datetime
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf




#'(timestamp) (funcName) (pathName:lineno) (process) (levelName) (name) [-] (instance+message)'
defaultFormat = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\S+) (\S+:\d+) (\d+) (\S+) (\S+) \[-\] (.*)'
#'(timestamp) (funcName) (pathName:lineno) (process) (levelName) (name) [(requestID) (user) (tenant)] (instance+message)'
contextFormat = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\S+) (\S+:\d+) (\d+) (\S+) (\S+) \[(\S+) (\S+) (\S+)\] (.*)'
#'(timestamp) (funcName) (pathName:lineno) (process) TRACE (name) (instance+traceback)'
traceFormat = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\S+) (\S+:\d+) (\d+) TRACE (\S+) (.*)'


def parseOpenstackLogLine(logline):
	# default format
	if '[-]' in logline:
		match = re.match(defaultFormat, logline)
		if match:
                        return (Row(
                                timestamp = match.group(1),
                                funcName = match.group(2),
                                pathNameLineno = match.group(3),
                                process = match.group(4),
                                levelName = match.group(5),
                                name = match.group(6),# i.e, neutron.agent.linux.ovs_lib 
                                content = match.group(7),
				requestID = None,
				user = None,
				tenant = None
				), 1)
		else:
			return(logline, 0)
	# trace format
	elif 'TRACE' in logline:
		match = re.match(traceFormat, logline)
		if match:
			return (Row(
				timestamp = match.group(1),
                                funcName = match.group(2),
                                pathNameLineno = match.group(3),
                                process = match.group(4),
                                levelName = 'TRACE',
                                name = match.group(5),# i.e, neutron.agent.linux.ovs_lib 
                                content = match.group(6),
                                requestID = None,
                                user = None,
                                tenant = None
				), 1)
		else:
			return (logline, 0)
	# context format			
	else:
		match = re.match(contextFormat, logline)
		if match:
			return (Row(
                                timestamp = match.group(1),
                                funcName = match.group(2),
                                pathNameLineno = match.group(3),
                                process = match.group(4),
                                levelName = match.group(5),
                                name = match.group(6),# i.e, neutron.agent.linux.ovs_lib 
                                requestID = match.group(7),
                                user = match.group(8),
                                tenant = match.group(9),
				content = match.group(10)
                                ), 1)
		else:
			return (logline, 0)



def commonUuid(logline):
	uuidPtn = r'[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}'
        uuids = set(re.findall(uuidPtn, logline))
        commUuids = uuids.intersection(broadcastUuids.value)
	return (commUuids, logline)

def pairLogUuid(uuidLog):
	"input: uuidLog is ([uuid], parsedLog)"
	res = [None] * len(uuidLog[0])
	i = 0
	for uuid in uuidLog[0]:
		res[i] = (uuid, uuidLog[1])
		i += 1	
	return res	



if __name__ == "__main__":
        print(sys.argv)
        if len(sys.argv) != 4:
                print("Usage: log_spark.py <logPathIn> <logPathOut> <uuidPathIn>")
                exit(-1)

        conf = SparkConf()
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.setAppName("PythonLog")
        sc = SparkContext(conf=conf)

        logPathIn = sys.argv[1]
       	logPathOut = sys.argv[2]
        uuidPathIn = sys.argv[3]
        print('logPathIn = %s, logPathOut = %s, uuidPathIn = %s' % (logPathIn, logPathOut, uuidPathIn))

#        numPartitions = 512 # we have 32 worker node with 16 cores each, not required if read from HDFS

        uuids = sc.textFile(uuidPathIn).collect()
        broadcastUuids = sc.broadcast(uuids)

        logRDD = sc.textFile(logPathIn + '/*/*') # i.e. there are two recursive levels: log_0616/log-012/nova/nova.log
	
	logUuidRDD = (logRDD
			.map(commonUuid)
			.filter(lambda (commUuids, logline): len(commUuids) > 0) # only keep logline that shares uuid with uuidPathIn
			)

	logUuidParsedRDD = (logUuidRDD
				.mapValues(parseOpenstackLogLine)
				.filter(lambda (commUuids, logTuple): logTuple[1] == 1) # only keep logline conform to the defined formats
				.mapValues(lambda logTuple: logTuple[0] ) 
				)

	logUuidParsedGroupRDD = (logUuidParsedRDD
				.flatMap(pairLogUuid)
				.groupByKey()
#				.mapValues(lambda log: [log]) # decrease the shuffle network flow with reduceByKey
#				.reduceByKey(lambda p, q: p + q)
				)

	logUuidParsedGroupSortedRDD = logUuidParsedGroupRDD.mapValues(lambda logs: sorted(logs, key = (lambda log: log.timestamp), reverse = False))

	logUuidParsedGroupSortedRDD.saveAsTextFile(logPathOut)

        sc.stop()

