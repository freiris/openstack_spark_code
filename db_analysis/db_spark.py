from __future__ import print_function
import re
import sys
import datetime
from pyspark.sql import Row
from pyspark import SparkContext, SparkConf


recordFormat = r'^(\d+),"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})","(\S+)","(\S+)",(.*)' # (id,timestamp,event,tablename,content)

def parseRecord(recordLine):
	match = re.match(recordFormat, recordLine)
	return Row(
		id = match.group(1),
		timestamp = match.group(2),
		event = match.group(3),
		tablename = match.group(4),
		content = match.group(5)
		)			

def commonUuid(recordLine):
	uuidPtn = r'[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}'
        uuids = set(re.findall(uuidPtn, recordLine))
        commUuids = uuids.intersection(broadcastUuids.value)
	return (commUuids, recordLine)

def pairRecordUuid(uuidRecord):
	"input: uuidLog is ([uuid], record)"
	res = [None] * len(uuidRecord[0])
	i = 0
	for uuid in uuidRecord[0]:
		res[i] = (uuid, uuidRecord[1])
		i += 1	
	return res	


if __name__ == "__main__":
        print(sys.argv)
        if len(sys.argv) != 4:
                print("Usage: record_spark.py <dbPathIn> <dbPathOut> <uuidPathIn>")
                exit(-1)

        conf = SparkConf()
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.setAppName("PythonDB")
        sc = SparkContext(conf=conf)

        dbPathIn = sys.argv[1]
       	dbPathOut = sys.argv[2]
        uuidPathIn = sys.argv[3]
        print('dbPathIn = %s, dbPathOut = %s, uuidPathIn = %s' % (dbPathIn, dbPathOut, uuidPathIn))

#        numPartitions = 512 # we have 32 worker node with 16 cores each, not required if read from HDFS

        uuids = sc.textFile(uuidPathIn).collect()
        broadcastUuids = sc.broadcast(uuids)

        recordRDD = sc.textFile(dbPathIn)
	
	recordUuidRDD = (recordRDD
#			.map(lambda line: line.split(',')) # csv file
			.map(commonUuid)
			.filter(lambda (commUuids, recordLine): len(commUuids) > 0) # only keep record that shares uuid with uuidPathIn
			.mapValues(parseRecord)
			)

	recordUuidGroupRDD = (recordUuidRDD
				.flatMap(pairRecordUuid)
				.groupByKey()
#				.mapValues(lambda record: [record]) # decrease the shuffle network flow with reduceByKey
#				.reduceByKey(lambda p, q: p + q)
				.cache()
				)


	recordUuidGroupSortedRDD = (recordUuidGroupRDD
					.mapValues(lambda records: sorted(records, key = (lambda record: record.timestamp), reverse = False))
#					.cache()
					)

	recordUuidGroupSortedRDD.saveAsTextFile(dbPathOut)

        sc.stop()

