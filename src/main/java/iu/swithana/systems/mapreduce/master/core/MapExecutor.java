package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.common.ResultMap;
import iu.swithana.systems.mapreduce.util.FileManager;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * This class executes all the Map tasks and supports fault tolerance in the process.
 */
public class MapExecutor {
    private static Logger logger = LoggerFactory.getLogger(MapExecutor.class);

    private Class mapperClass;
    private String inputDirectory;
    private Hashtable<String, WorkerRMI> workerTable;
    private BlockingQueue<String> idleWorkers;
    private BlockingQueue<File> fileQueue;
    private Map<String, String> inProgressWorkers;
    private int numbeOfWorkers;

    public MapExecutor(Class mapperClass, String inputDirectory, Hashtable<String, WorkerRMI> workerTable) {
        this.workerTable = workerTable;
        this.mapperClass = mapperClass;
        this.inputDirectory = inputDirectory;
        this.idleWorkers = new ArrayBlockingQueue<>(workerTable.keySet().size());
        this.numbeOfWorkers = workerTable.keySet().size();
        this.inProgressWorkers = new Hashtable<>();
    }

    public ResultMap runJob() {
        // prepare the input files list into the queue
        FileManager fileManager = new FileManager();
        List<File> filesInDirectory = fileManager.getFilesInDirectory(inputDirectory);
        fileQueue = new ArrayBlockingQueue<>(filesInDirectory.size());
        fileQueue.addAll(filesInDirectory);

        final MapResultsHandler mapResultsHandler = new MapResultsHandler();

        // add all the workers to the idle queue
        for (String workerID : workerTable.keySet()) {
            idleWorkers.add(workerID);
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
                if (fileQueue.isEmpty()) {
                    if (inProgressWorkers.isEmpty()) {
                        // all the tasks are done
                        break;
                    } else {
                        // tasks are running, wait to check if any tasks fail
                        Thread.sleep(1000);
                        continue;
                    }
                }
                File file = fileQueue.take();
                // if it's a directory, skip
                if (file.isFile()) {
                    // blocks on the idle queue, waits for an available worker
                    final String workerID = idleWorkers.take();
                    WorkerRMI worker = workerTable.get(workerID);
                    logger.debug("Submitting file " + file.getName() + " to worker: " + workerID);
                    inProgressWorkers.put(workerID, file.getName());
                    executor.submit(new MapperTask(file, worker, mapperClass, fileManager, workerID,
                            new MapResultListener() {
                                // in case of successful execution, save the result
                                @Override
                                public void onResult(ResultMap resultMap, String workerID) {
                                    mapResultsHandler.addResult(resultMap);
                                    inProgressWorkers.remove(workerID);
                                    idleWorkers.add(workerID);
                                }

                                // if the execution fails, retry for the same file
                                @Override
                                public void onError(Exception e, String workerID, File file) {
                                    logger.error("Error accessing Worker: " + workerID +
                                            ". Assuming it's inaccessible and dropping the worker. " + e.getMessage(), e);
                                    logger.error("Adding file: " + file.getName() + " to the fileList.");
                                    fileQueue.add(file);
                                    inProgressWorkers.remove(workerID);
                                }
                            }));
                }
            }
            // wait till all the threads have been completed, then cleanup.
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);

        } catch (Exception e) {
            logger.error("Error accessing the mapper function: " + e.getMessage(), e);
        }
        return mapResultsHandler.getResultsList();
    }
}
