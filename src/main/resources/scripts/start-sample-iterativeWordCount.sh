#!/bin/sh
JAR_LOCATION=./lib
CONFIG_LOCATION=./config/config.properties
LOGS_LOCATION=./logs

mkdir -p $LOGS_LOCATION

if wordcountid=$(pgrep -f iu.swithana.mapreduce.sample.iterativeWordCount.jar)
then
    echo "Iterative Word Count is running, pid is $wordcountid"
else
    echo "Starting the Iterative Word Count Sample"
    java -Dconfig.file=$CONFIG_LOCATION -jar $JAR_LOCATION/iu.swithana.mapreduce.sample.iterativeWordCount.jar >> $LOGS_LOCATION/sample_iterative.log &
    wordcountid=$(pgrep -f iu.swithana.mapreduce.sample.iterativeWordCount.jar)
    echo "Started the Iterative Word count application with pid: $wordcountid"
fi
