"""
Integrate libvirt/ovs state with log, message and database within 300 seconds.
Output ((uuid, timestamp), (state, (log, msg, db))) tuples.
"""

from __future__ import print_function
import re
import sys
import ast
import datetime
#from pyspark.sql import Row
from pyspark import SparkContext, SparkConf

def timeBefore(timestamp, numSec):
        timeFmt = "%Y-%m-%d %H:%M:%S.%f" # state timestamp has milisecond
        current = datetime.datetime.strptime(timestamp, timeFmt)
        before = current - datetime.timedelta(seconds=numSec)
        return before.strftime(timeFmt)[0:-3]


def selectByTimestamp(timestampOperation):
        # both timestamps and operations are previously sorted
        timestamps = timestampOperation[0]
        operations = timestampOperation[1]
	if operations is None: # leftOuterJoin may leave operations as None
		return map(lambda ts: (ts, [None]), timestamps)
        seconds = 300 # in second
        numTS = len(timestamps)
        numOP = len(operations)
        res = [None] * numTS

        k = 0
        for i in xrange(numTS):
                ops = []
                ts1 = timestamps[i]
                ts0 = timeBefore(ts1, seconds)
                # optimize to avoid scanning operations from the begin
                if (operations[0]['timestamp'] > ts1) or (operations[-1]['timestamp'] < ts0): # out-of-range case
                        pass # res[i] assignment at the end
                else:
                        referOperation = operations[k]
                        if referOperation['timestamp'] >= ts0:
                                for j0 in xrange(k, -1, -1):
                                        if operations[j0]['timestamp'] >= ts0: # referOperation added at first
                                                ops.append(operations[j0]) # decreasing-time order
                                        else: # ignore operations before ts0
                                                break
                                ops = ops[::-1] # increasing-time order 
                        for j1 in xrange(k+1, numOP):
                                if operations[j1]['timestamp'] < ts0: # skip operations before ts0 
                                        continue
                                if operations[j1]['timestamp'] <= ts1:
                                        ops.append(operations[j1])
                                else: # ignore operations after ts1
                                        break
                        k = j1 - 1 # ensure that operations[k]['timestamp'] <= ts1 < ts1'. it holds even when j1 exhausts the xrange    
                res[i] = (ts1, ops)
        return res

		

if __name__ == "__main__":
        print(sys.argv)
        if len(sys.argv) != 6:
                print("Usage: integrate_spark.py <logPathIn> <msgPathIn> <dbPathIn> <statePathIn> <integratePathOut>") # state can be libvirt/ovs state
                exit(-1)

        conf = SparkConf()
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.setAppName("PythonIntegrate")
        sc = SparkContext(conf=conf)

        logPathIn = sys.argv[1]
       	msgPathIn = sys.argv[2]
        dbPathIn = sys.argv[3]
	statePathIn = sys.argv[4]
	integratePathOut = sys.argv[5]

        print('logPathIn = %s, msgPathIn = %s, dbPathIn = %s, statePathIn = %s, integratePathOut = %s'\
	% (logPathIn, msgPathIn, dbPathIn, statePathIn, integratePathOut))


	## read and parse
	logRDD = (sc
			.textFile(logPathIn)
			.map(ast.literal_eval) # may required to recognize string as tuple 
			)
	print('logRDD complete')

	msgRDD = (sc
			.textFile(msgPathIn)
			.map(ast.literal_eval) # may required to recognize string as tuple 
			)
	print('msgRDD complete')

	dbRDD = (sc
			.textFile(dbPathIn)
			.map(ast.literal_eval) # may required to recognize string as tuple 
			)
	print('dbRDD complete')

	stateRDD = (sc
			.textFile(statePathIn)
			.map(ast.literal_eval) # may required to recognize string as tuple 
			)	
	print('stateRDD complete')
	
	## select operations by timestamp from state
	timestampRDD = (stateRDD
			.map(lambda (uuid, stateList): (uuid, map(lambda state: state['timestamp'], stateList))) # the second map is a python function, not spark
			.cache()
			)

	tsMsgRDD = (timestampRDD
			.leftOuterJoin(msgRDD)
			.mapValues(selectByTimestamp)
			.flatMap(lambda (uuid, tsMsgList): map(lambda tsMsg: ((uuid, tsMsg[0]), tsMsg[1]), tsMsgList)) 
#			.cache() 
			)
	
	tsLogRDD = (timestampRDD
			.leftOuterJoin(logRDD)
			.mapValues(selectByTimestamp)
			.flatMap(lambda (uuid, tsLogList): map(lambda tsLog: ((uuid, tsLog[0]), tsLog[1]), tsLogList))
#			.cache()
			)
	tsDBRDD = (timestampRDD
			.leftOuterJoin(dbRDD)
			.mapValues(selectByTimestamp)
			.flatMap(lambda (uuid, tsDBList): map(lambda tsDB: ((uuid, tsDB[0]), tsDB[1]), tsDBList))	
#			.cache()
			)

	tsStateRDD = stateRDD.flatMap(lambda (uuid, stateList): map(lambda state: ((uuid, state['timestamp']), state), stateList)) 

	## put together by (uuid, timestamp)
	integrateRDD = (tsStateRDD
			.join(tsLogRDD) # (state, log)	
			.join(tsMsgRDD) # ((state, log), msg)
			.join(tsDBRDD) # (((state, log), msg), db)
			.mapValues(lambda x: (x[0][0][0], (x[0][0][1], x[0][1], x[1]))) # (state, (log, msg, db))
#			.sortByKey() # the key is (uuid, timestamp)
			)
	'''
	integrateSampleRDD = (integrateRDD
				.sample(False, 0.01, 2)
				.cache()
				)

	sample10 = integrateSampleRDD.take(10)
	for ele in sample10:
		print(ele)

	integrateSampleRDD.saveAsTextFile(integratePathOut)
	'''


	integrateRDD.saveAsTextFile(integratePathOut)

	sc.stop()
