#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
hadoop=/opt/module/hadoop-3.1.3/bin/hadoop
db=gmall

if [ -n "$1" ]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

sql="
use $db;
load data inpath '/origin_data/gmall/log/topic_log/$do_date' OVERWRITE into table ods_log partition(dt='$do_date');
"

$hive -e "$sql"
$hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer -Dmapreduce.job.queuename=hive /warehouse/gmall/ods/ods_log/dt=$do_date

