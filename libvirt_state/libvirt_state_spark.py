#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
The K-means algorithm written from scratch against PySpark. In practice,
one may prefer to use the KMeans algorithm in MLlib, as shown in
examples/src/main/python/mllib/kmeans.py.

This example requires NumPy (http://www.numpy.org/).
"""
from __future__ import print_function

import sys
import json

#import numpy as np
from pyspark import SparkContext, SparkConf



def state_diff3(VMStates):
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
    #sc = SparkContext(appName="PythonLibvirtState")
    sc = SparkContext(conf=conf)

    pathIn = sys.argv[1]
    pathOut = sys.argv[2]

    print('pathIn = %s, pathOut = %s' % (pathIn, pathOut))

    libvirtStates = sc.textFile(pathIn)
    libvirtParsedRDD = (libvirtStates
			.map(lambda line: json.loads(line.strip(',\n'))))
#            		.cache())
    libvirtReduceRDD = (libvirtParsedRDD
            		.map(lambda vmState: (vmState["uuid"], [vmState]))
            		.reduceByKey(lambda p, q: p + q))
#    libvirtReduceRDD = (libvirtParsedRDD
#			.map(lambda vmState: (vmState["uuid"], vmState))
#			.groupByKey())

    libvirtReduceSortedRDD = (libvirtReduceRDD
			      .map(lambda (uuid, vms): (uuid, sorted(vms, key = (lambda vm: vm["timestamp"]), reverse = False))))
#                	      .cache())
    libvirtReduceSortedDiffRDD = (libvirtReduceSortedRDD
                                  .map(lambda (uuid, vms): (uuid, state_diff3(vms))))
#                                  .cache())

    libvirtReduceSortedDiffRDD.map(lambda x: json.dumps(x)).saveAsTextFile(pathOut) # dump to json format, then save as textfile


    sc.stop()
