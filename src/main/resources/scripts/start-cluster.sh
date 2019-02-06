#!/bin/sh
JAR_LOCATION=./lib
CONFIG_LOCATION=../src/main/resources/config.properties
LOG_LOCATION=./logs

mkdir -p $LOG_LOCATION
if masterpid=$(pgrep -f iu.swithana.mapreduce.master.jar)
then
    echo "Master is running, pid is $masterpid"
else
    echo "Starting the master"
    java -jar $JAR_LOCATION/iu.swithana.mapreduce.master.jar -Dconfig.file=$CONFIG_LOCATION >> $LOG_LOCATION/master.log &
    masterpid=$(pgrep -f iu.swithana.mapreduce.master.jar)
    echo "Started Master with pid: $masterpid"
fi

sleep 2

COUNTER=0
while [[  $COUNTER -lt $1 ]]; do
    echo "Starting the worker $COUNTER"
    java -jar $JAR_LOCATION/iu.swithana.mapreduce.worker.jar -Dconfig.file=$CONFIG_LOCATION >> $LOG_LOCATION/worker_$COUNTER.log &
    let COUNTER=COUNTER+1
done



