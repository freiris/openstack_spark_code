#!/usr/bin/env python

import os
import sys


low = int(sys.argv[1])
high = int(sys.argv[2])

cmdPrefix = '~/spark-1.4.0/bin/spark-submit  --master spark://spark4-master:7077 --driver-memory 20g --executor-memory 14g msg_spark_opt.py '
msgPathIn = 'hdfs://spark4-master:9000/user/yong/msg_analysis/msg_0616/'
msgPathOut = 'hdfs://spark4-master:9000/user/yong/msg_analysis/msg_0616_ovs_res/'
uuidPathIn = 'hdfs://spark4-master:9000/user/yong/ovs_state/ovs_state_0616_uuid'
#msgPathOut = 'hdfs://spark4-master:9000/user/yong/msg_analysis/msg_0616_libvirt_res/'
#uuidPathIn = 'hdfs://spark4-master:9000/user/yong/libvirt_state/libvirt_state_0616_uuid'

for i in xrange(low, high + 1):
	index = '%02d' % i
	print index
	splitIndex = 'split_' + index
	msgPathInSplit = msgPathIn + splitIndex 
	msgPathOutSplit = msgPathOut + splitIndex
	cmd = cmdPrefix + ' ' + msgPathInSplit + ' ' + msgPathOutSplit + ' ' + uuidPathIn
	
	print cmd
	os.system(cmd)

print 'COMPLETE'	
