#!/bin/sh
if masterpid=$(pgrep -f iu.swithana.mapreduce.master.jar)
then
    echo "Master is running, pid is $masterpid"
    echo "Stoping master..."
    kill $masterpid
else
    echo "Master has already stopped!"
fi

sleep 2

for workerid in $(pgrep -f 'iu.swithana.mapreduce.worker.jar'); do
    echo "Stopping the worker $workerid"
    kill "$workerid"
done


