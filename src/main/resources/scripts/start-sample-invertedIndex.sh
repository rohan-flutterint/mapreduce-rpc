#!/bin/sh
JAR_LOCATION=./lib
CONFIG_LOCATION=./config/config.properties
LOGS_LOCATION=./logs

mkdir -p $LOGS_LOCATION

if invIndexid=$(pgrep -f iu.swithana.mapreduce.sample.invertedIndex.jar)
then
    echo "Inverted Index is running, pid is $invIndexid"
else
    echo "Starting the Inverted Index Sample"
    java -jar $JAR_LOCATION/iu.swithana.mapreduce.sample.invertedIndex.jar -Dconfig.file=$CONFIG_LOCATION >> $LOGS_LOCATION/sample_invertedIndex.log &
    invIndexid=$(pgrep -f iu.swithana.mapreduce.sample.invertedIndex.jar)
    echo "Started the Inverted index application with pid: $invIndexid"
fi
