package iu.swithana.systems.mapreduce.master.core;

import com.google.common.collect.Iterables;
import iu.swithana.systems.mapreduce.common.ResultMap;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class ReducerExecutor {
    private static Logger logger = LoggerFactory.getLogger(ReducerExecutor.class);

    private ResultMap resultMap;
    private Hashtable<String, WorkerRMI> workerTable;
    private BlockingQueue<String> idleWorkers;
    private int numbeOfWorkers, partitionSize;
    private Class reducerClass;

    public ReducerExecutor(ResultMap resultMap, Hashtable<String, WorkerRMI> workerTable, Class reducerClass,
                           int partitionSize) {
        this.resultMap = resultMap;
        this.workerTable = workerTable;
        this.numbeOfWorkers = workerTable.keySet().size();
        this.reducerClass = reducerClass;
        this.idleWorkers = new ArrayBlockingQueue<>(workerTable.keySet().size());
        this.partitionSize = partitionSize;
    }

    public Map<String, String> runJob() {
        ReducerResultsHandler resultsHandler = new ReducerResultsHandler();
        // add all the workers to the idle queue
        for (String workerID : workerTable.keySet()) {
            idleWorkers.add(workerID);
        }

        // partition the key set
        Set<String> keyList = resultMap.getKeys();
        Iterator<List<String>> partitions = Iterables.partition(keyList, partitionSize).iterator();

        // run the thread pool, each per worker
        try {
            ExecutorService executor = Executors.newFixedThreadPool(numbeOfWorkers);
            while (partitions.hasNext()) {
                List<String> partition = partitions.next();
                // blocks on the idle queue, waits for an available worker
                final String workerID = idleWorkers.take();
                WorkerRMI worker = workerTable.get(workerID);
                logger.debug("Submitting key set: " + partition + " to worker: " + workerID);
                ResultMap subMap = resultMap.getSubMap(partition);
                executor.submit(new ReducerTask(subMap, reducerClass, worker, workerID,
                        new ReducerResultListener() {
                            @Override
                            public void onResult(Map<String, String> result, String workerID) {
                                resultsHandler.addResult(result);
                                idleWorkers.add(workerID);
                            }

                            @Override
                            public void onError(Exception e, String workerID, Set<String> keyset) {
                                logger.error("Error accessing Worker: " + workerID +
                                        ". Assuming it's inaccessible and dropping the worker. " + e.getMessage(), e);
                                // todo: create the retry scenario on worker failure to resubmit keys
                            }
                        }));
            }

            // wait till all the threads have been completed, then cleanup.
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);

        } catch (Exception e) {
            logger.error("Error accessing the mapper function: " + e.getMessage(), e);
        }
        return resultsHandler.getAllResults();
    }

}
