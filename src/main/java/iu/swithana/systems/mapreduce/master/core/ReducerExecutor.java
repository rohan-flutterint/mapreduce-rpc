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

    private Hashtable<String, WorkerRMI> workerTable;
    private BlockingQueue<String> idleWorkers;
    private Map<String, String> inProgressWorkers;
    private BlockingQueue<String> partitionQueue;
    private int numbeOfWorkers, partitionSize;
    private Class reducerClass;
    private String jobID;

    public ReducerExecutor(Hashtable<String, WorkerRMI> workerTable, Class reducerClass,
                           int partitions, String jobID) {
        this.workerTable = workerTable;
        this.numbeOfWorkers = workerTable.keySet().size();
        this.reducerClass = reducerClass;
        this.idleWorkers = new ArrayBlockingQueue<>(workerTable.keySet().size());
        this.partitionQueue = new ArrayBlockingQueue(partitions);
        this.partitionSize = partitions;
        this.inProgressWorkers = new Hashtable<>();
        this.jobID = jobID;
    }

    public Map<String, String> runJob() {
        ReducerResultsHandler resultsHandler = new ReducerResultsHandler();
        // add all the workers to the idle queue
        for (String workerID : workerTable.keySet()) {
            idleWorkers.add(workerID);
        }

        // add partition ids to the partition queue
        for (int i = 0; i < partitionSize; i++) {
            partitionQueue.add(String.valueOf(i));
        }

        // run the thread pool, each per worker
        try {
            ExecutorService executor = Executors.newFixedThreadPool(numbeOfWorkers);
            while (true) {
                /**
                 * Fault tolerance mechanism
                 * If all the workers are done and no work left, the work is done.
                 * If not, wait till all the workers finish and check if the work is done or not.
                 */
                if (partitionQueue.isEmpty()) {
                    if (inProgressWorkers.isEmpty()) {
                        // all the tasks are done
                        break;
                    } else {
                        // tasks are running
                        Thread.sleep(1000);
                        continue;
                    }
                }

                // blocks on the data queue, for fault tolerance
                String partition = partitionQueue.take();

                // blocks on the idle queue, waits for an available worker
                final String workerID = idleWorkers.take();
                WorkerRMI worker = workerTable.get(workerID);
                logger.info("Submitting partition: " + partition + " to worker: " + workerID);

                inProgressWorkers.put(workerID, partition);
                executor.submit(new ReducerTask(partition, reducerClass, worker, workerID, jobID,
                        new ReducerResultListener() {
                            @Override
                            public void onResult(Map<String, String> result, String workerID) {
                                resultsHandler.addResult(result);
                                inProgressWorkers.remove(workerID);
                                idleWorkers.add(workerID);
                            }
                            @Override
                            public void onError(Exception e, String workerID, String partition) {
                                logger.error("Error accessing Worker: " + workerID +
                                        ". Assuming it's inaccessible and dropping the worker. " + e.getMessage(), e);
                                logger.info("Resubmitting the task to the task queue for partition: " + partition);
                                partitionQueue.add(partition);
                                inProgressWorkers.remove(workerID);
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
