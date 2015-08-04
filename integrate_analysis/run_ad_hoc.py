#!/usr/bin/env python

import os
import sys


low = int(sys.argv[1])
high = int(sys.argv[2])

cmdPrefix = '~/spark-1.4.0/bin/spark-submit  --master spark://spark4-master:7077 --driver-memory 20g --executor-memory 14g integrate_spark.py '
#statePathIn = 'hdfs://spark4-master:9000/user/yong/libvirt_state/libvirt_state_0616_res_opt_adjust/' # libvirt_state
#logPathIn = 'hdfs://spark4-master:9000/user/yong/log_analysis/log_0616_libvirt_res_json/'
#msgPathIn = 'hdfs://spark4-master:9000/user/yong/msg_analysis/msg_0616_libvirt_res/'
#dbPathIn = 'hdfs://spark4-master:9000/user/yong/db_analysis/db_0616_libvirt_res_json/'
#integratePathOut = 'hdfs://spark4-master:9000/user/yong/integrate_analysis/integrate_0616_res_out/'

statePathIn = 'hdfs://spark4-master:9000/user/yong/ovs_state/ovs_state_0616_iface_id' # ovs_state
logPathIn = 'hdfs://spark4-master:9000/user/yong/log_analysis/log_0616_ovs_res_json/'
msgPathIn = 'hdfs://spark4-master:9000/user/yong/msg_analysis/msg_0616_ovs_res/'
dbPathIn = 'hdfs://spark4-master:9000/user/yong/db_analysis/db_0616_ovs_res_json/'
integratePathOut = 'hdfs://spark4-master:9000/user/yong/integrate_analysis/integrate_0616_ovs_out/'

for i in xrange(low, high + 1):
	index = '%02d' % i
	print index
	splitIndex = 'split_' + index
	msgPathInSplit = msgPathIn + splitIndex 
	integratePathOutSplit = integratePathOut + splitIndex
	cmd = cmdPrefix + ' ' + logPathIn + ' ' + msgPathInSplit + ' ' + dbPathIn + ' ' + statePathIn + ' ' +  integratePathOutSplit
	
	print cmd
	os.system(cmd)

print 'COMPLETE'	
