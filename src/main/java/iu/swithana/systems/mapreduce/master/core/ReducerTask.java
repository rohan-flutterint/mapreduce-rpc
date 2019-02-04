package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.common.ResultMap;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ReducerTask implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(ReducerTask.class);

    private ResultMap subMap;
    private Class reducerClass;
    private WorkerRMI worker;
    private String workerID;
    private ReducerResultListener reducerResultListener;

    public ReducerTask(ResultMap subMap, Class reducerClass, WorkerRMI worker, String workerID,
                       ReducerResultListener reducerResultListener) {
        this.subMap = subMap;
        this.reducerClass = reducerClass;
        this.worker = worker;
        this.workerID = workerID;
        this.reducerResultListener = reducerResultListener;
    }

    @Override
    public void run() {
        try {
            Map<String, String> resultMap = worker.doReduce(subMap, reducerClass);
            reducerResultListener.onResult(resultMap, workerID);
        } catch (Exception e) {
            logger.error("Exception occurred in completing the reducer the job on worker: " + this.workerID + " " +
                    e.getMessage(), e);
            reducerResultListener.onError(e, workerID, subMap.getKeys());
        }
    }
}
