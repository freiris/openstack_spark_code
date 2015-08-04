#!/usr/bin/env python

import os
import sys

# [2:75] + [78:80] + [83:125]
low = int(sys.argv[1])
high = int(sys.argv[2])

cmdPrefix = '~/spark-1.4.0/bin/spark-submit  --master spark://spark4-master:7077 --driver-memory 20g --executor-memory 14g ovs_state_snapshot_unique_opt.py '
ovsPathIn = 'hdfs://spark4-master:9000/user/yong/ovs_state/ovs_state_0616/'
ovsPathOut = 'hdfs://spark4-master:9000/user/yong/ovs_state/ovs_state_0616_res/'

for i in xrange(low, high + 1):
	splitIndex = '10.1.0.%d.out' % i
	ovsPathInSplit = ovsPathIn + splitIndex 
	ovsPathOutSplit = ovsPathOut + splitIndex
	cmd = cmdPrefix + ' ' + ovsPathInSplit + ' ' + ovsPathOutSplit
	
	print cmd
	os.system(cmd)

print 'COMPLETE'	
