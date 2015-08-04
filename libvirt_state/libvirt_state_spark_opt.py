from __future__ import print_function

import sys
import json

from pyspark import SparkContext, SparkConf



def state_diff(VMStates):
        uniqVMStates = []
#       print 'daertsrfa daertesryfrgare'
        referState = VMStates[0]
#       print referState
#       print '--------------------diff-----------------------'
        for state in VMStates[1:]:
             for key in referState:
                     if key != "timestamp":
                             if referState[key] != state[key]:
                                     uniqVMStates.append(referState)
#                                    print referState[key], state[key]
                                     referState = state
                                     break
        uniqVMStates.append(referState)
#       print referState
        return uniqVMStates




if __name__ == "__main__":

    print(sys.argv)

    if len(sys.argv) != 3:
        print("Usage: libvirt_state_spark <pathIn> <pathOut>")
        exit(-1)

    conf = SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    #conf.set("spark.executor.extraJavaOptions", "-Xms=2g -Xmx=2g")
#    conf.set("spark.driver.memory", "4g")
#    conf.set("spark.executor.memory", "2g")
    conf.setAppName("PythonLibvirtState")
    sc = SparkContext(conf=conf)

    pathIn = sys.argv[1]
    pathOut = sys.argv[2] 
    numPartitions = 256 # we have 32 worker node with 8 cores each

    print('pathIn = %s, pathOut = %s' % (pathIn, pathOut))

    libvirtStates = sc.textFile(pathIn, numPartitions)
    libvirtParsedRDD = (libvirtStates
			.map(lambda line: json.loads(line.strip(',\n'))))
#            		.cache())

# reduceByKey leads to many retrials, and finally aborted. why?
#    libvirtReduceRDD = (libvirtParsedRDD
#            		.map(lambda vmState: (vmState["uuid"], [vmState]))
#            		.reduceByKey(lambda p, q: p + q))

    libvirtReduceRDD = (libvirtParsedRDD
			.map(lambda vmState: (vmState["uuid"], vmState))
			.groupByKey())

    libvirtReduceSortedRDD = (libvirtReduceRDD
			      .mapValues(lambda vms: sorted(vms, key = (lambda vm: vm["timestamp"]), reverse = False)))
#			      .cache()	

#    libvirtReduceSortedRDD = (libvirtReduceRDD
#			      .map(lambda (uuid, vms): (uuid, sorted(vms, key = (lambda vm: vm["timestamp"]), reverse = False))))
##                	      .cache())

    libvirtReduceSortedDiffRDD = (libvirtReduceSortedRDD
				  .mapValues(lambda vms: state_diff(vms)))
#			          .persist()

#    libvirtReduceSortedDiffRDD = (libvirtReduceSortedRDD
#                                  .map(lambda (uuid, vms): (uuid, state_diff(vms))))
##                                  .cache())


#    libvirtReduceSortedDiffRDD.map(lambda x: json.dumps(x)).saveAsTextFile(pathOut) # dump to json format, then save as textfile
    libvirtReduceSortedDiffRDD.saveAsTextFile(pathOut) 
    sc.stop()
