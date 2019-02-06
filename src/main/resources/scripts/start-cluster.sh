#!/bin/sh
JAR_LOCATION=./lib
CONFIG_LOCATION=./config/config.properties
LOG_LOCATION=./logs

mkdir -p $LOG_LOCATION

if [[ $# -eq 0 ]] ; then
    echo 'Please specify the number of worker nodes required as the first parameter!'
    exit 1
fi

if masterpid=$(pgrep -f iu.swithana.mapreduce.master.jar)
then
    echo "Master is running, pid is $masterpid"
else
    echo "Starting the master"
    java -Dconfig.file=$CONFIG_LOCATION -jar $JAR_LOCATION/iu.swithana.mapreduce.master.jar >> $LOG_LOCATION/master.log &
    masterpid=$(pgrep -f iu.swithana.mapreduce.master.jar)
    echo "Started Master with pid: $masterpid"
fi

sleep 2

COUNTER=0
while [[  $COUNTER -lt $1 ]]; do
    echo "Starting the worker $COUNTER"
    java -Dconfig.file=$CONFIG_LOCATION -jar $JAR_LOCATION/iu.swithana.mapreduce.worker.jar >> $LOG_LOCATION/worker_$COUNTER.log &
    let COUNTER=COUNTER+1
done