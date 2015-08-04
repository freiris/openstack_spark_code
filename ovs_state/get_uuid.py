from __future__ import print_function
import sys
from ast import literal_eval
from pyspark import SparkContext, SparkConf



if __name__ == "__main__":

    	print(sys.argv)

    	if len(sys.argv) != 3:
        	print("Usage: get_uuid.py <pathIn> <pathOut>")
        	exit(-1)
	
    	conf = SparkConf()
    	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    	conf.setAppName("PythonGetUuid")
    	sc = SparkContext(conf=conf)

    	pathIn = sys.argv[1]
    	pathOut = sys.argv[2] 

    	print('pathIn = %s, pathOut = %s' % (pathIn, pathOut))

    	baseRDD = sc.textFile(pathIn) # _SUCCESS does not matter?
    	uuidRDD = (baseRDD
			.map(literal_eval) # parse string into tuple
			.map(lambda x: x[0])) # extract the uuid 
    	uuidRDD.saveAsTextFile(pathOut)
    	sc.stop()	
