#!/bin/sh
JAR_LOCATION=./lib
CONFIG_LOCATION=./config/config.properties
LOGS_LOCATION=./logs

mkdir -p $LOGS_LOCATION

if wordcountid=$(pgrep -f iu.swithana.mapreduce.sample.wordCount.jar)
then
    echo "Word Count is running, pid is $wordcountid"
else
    echo "Starting the Word Count Sample"
    java -Dconfig.file=$CONFIG_LOCATION -jar $JAR_LOCATION/iu.swithana.mapreduce.sample.wordCount.jar >> $LOGS_LOCATION/sample_wordcount.log &
    wordcountid=$(pgrep -f iu.swithana.mapreduce.sample.wordCount.jar)
    echo "Started the Wordcount application with pid: $wordcountid"
fi
