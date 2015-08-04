"""
Remove adjacent/sucessive duplicated OVS states (ovsdb snapshots) while ignoring the frequently changing 'statistics' field.
Specifically, we remove the repeating identical states, and only keep the distinct ones. i.e. [s1, s1, s2, s3, s1, s1, s4] => [s1, s2, s3, s1, s4] 
If a table in the ovsdb snapshot (OVS state) contains the 'statistics' field, we ignore the changing of the 'statistics'.
"""

from __future__ import print_function
import sys
import json
from pyspark import SparkContext, SparkConf

'''
def unique(ovsStates):
    uniqovsStates=[]
    referState=ovsStates[0]
    for state in ovsStates[1:]:
	for key in referState:
	    if key != 'timestamp' and  key != 'IP':
		if differ(referState[key], state[key]):
		    uniqovsStates.append(referState)
		    referState = state
		    break
    uniqovsStates.append(referState)
    return uniqovsStates
'''

def iterUnique(ovsStates):
	try:
		referState = ovsStates.next() # without assuming non-empty
	except:
		referState = None
		
	for state in ovsStates:
		for key in referState:
			if key != 'timestamp' and  key != 'IP':
				if differ(referState[key], state[key]):
					yield referState
					referState = state
					break
	else:
		yield referState # None if empty ovsStates




def differ(table1, table2):
	headings = table1['headings']
	idx = len(headings)# never visit this index by default
	if 'statistics' in headings: # we ignore the frequently changed statistics filed
		idx = headings.index('statistics')
	diffRecord = False 
	for record1, record2 in zip(table1['data'], table2['data']):
		for i in xrange(len(headings)):	
			if i != idx: # if 'statisitics' isn't in headings, default True (since i < idx); otherwise, we skip this index
				if record1[i] != record2[i]:
					diffRecord = True
					break
		if diffRecord: # found a pair of different records, so the two tables are different
			return True # Early return 
	return False # No different record-pair found, so the two tables are NOT different   	
						



if __name__ == "__main__":
    	if len(sys.argv) != 3:
        	print("Usage: ovs_state_snapshot_unique_opt.py <pathIn> <pathOut>")
        	exit(-1)

    	conf = SparkConf()
    	conf.setAppName("PythonOvsState")
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.speculation", "true")
    	sc = SparkContext(conf=conf)

    	pathIn = sys.argv[1]
    	pathOut = sys.argv[2]

    	print('pathIn = %s, pathOut = %s' % (pathIn, pathOut))

    	ovsRDD = sc.textFile(pathIn) # the input is the ovsdb snapshots on a physical machine which are inherently ordered by time.
    	ovsParsedRDD = ovsRDD.map(lambda line: json.loads(line.strip(',\n')))
	ovsUniqRDD = (ovsParsedRDD
			.mapPartitions(iterUnique)
			.coalesce(1)
			.mapPartitions(iterUnique)
			)

	ovsUniqRDD.saveAsTextFile(pathOut)
#	ovsReduceRDD = (ovsParsedRDD
#		    	.map(lambda ovsState: (ovsState["IP"],[ovsState]))
#		    	.reduceByKey(lambda p, q: p + q)
#		    	)
#    	ovsReduceUniqueRDD = ovsReduceRDD.mapValues(lambda ovsStates: unique(ovsStates))
#    	ovsReduceUniqueRDD.saveAsTextFile(pathOut)
    
    	sc.stop()
