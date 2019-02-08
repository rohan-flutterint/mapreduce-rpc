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
    private Set<File> completedTasks;
    private Set<File> inProgressTasks;
    private int totalTasks;
    private String jobID;

    public MapExecutor(Class mapperClass, String inputDirectory, Hashtable<String, WorkerRMI> workerTable, String jobID) {
        this.workerTable = workerTable;
        this.mapperClass = mapperClass;
        this.inputDirectory = inputDirectory;
        this.idleWorkers = new ArrayBlockingQueue<>(workerTable.keySet().size());
        this.numbeOfWorkers = workerTable.keySet().size();
        this.inProgressWorkers = new Hashtable<>();
        this.completedTasks = Collections.synchronizedSet(new HashSet<>());
        this.inProgressTasks = Collections.synchronizedSet(new HashSet<>());
        this.jobID = jobID;
    }

    public ResultMap runJob() {
        // prepare the input files list into the queue
        FileManager fileManager = new FileManager();
        List<File> filesInDirectory = fileManager.getFilesInDirectory(inputDirectory);
        fileQueue = new ArrayBlockingQueue<>(filesInDirectory.size());
        fileQueue.addAll(filesInDirectory);
        totalTasks = fileQueue.size();

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
                        // straggler prevention
                        if (!areAllTasksDone() && getTaskProgress() > 95) {
                            fileQueue.addAll(getInProgressTasks());
                        }
                        // if tasks are running wait until they finish to retry if one fails
                        Thread.sleep(1000);
                        continue;
                    }
                }
                File file = fileQueue.take();
                // if it's a directory, skip
                if (file.isFile()) {
                    // if it's already been completed, continue
                    if (alreadyCompleted(file)) {
                        continue;
                    }

                    // blocks on the idle queue, waits for an available worker
                    final String workerID = idleWorkers.take();
                    WorkerRMI worker = workerTable.get(workerID);
                    logger.info("Submitting file " + file.getName() + " to worker: " + workerID);
                    inProgressWorkers.put(workerID, file.getName());
                    addInProgressTask(file);
                    executor.submit(new MapperTask(file, worker, mapperClass, fileManager, workerID, jobID,
                            new MapResultListener() {
                                // in case of successful execution, save the result
                                @Override
                                public void onResult(ResultMap resultMap, String workerID, File file) {
                                    inProgressWorkers.remove(workerID);
                                    idleWorkers.add(workerID);
                                    addCompletedTask(file);
                                    removeInProgressTask(file);
                                }

                                // if the execution fails, retry for the same file
                                @Override
                                public void onError(Exception e, String workerID, File file) {
                                    logger.error("Error accessing Worker: " + workerID +
                                            ". Assuming it's inaccessible and dropping the worker. " + e.getMessage(), e);
                                    logger.error("Adding file: " + file.getName() + " to the task queue again.");
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

    synchronized private void addCompletedTask(File file) {
        if (!alreadyCompleted(file)) {
            completedTasks.add(file);
        }
    }

    synchronized private void addInProgressTask(File file) {
        inProgressTasks.add(file);
    }

    synchronized private void removeInProgressTask(File file) {
        inProgressTasks.remove(file);
    }

    synchronized private Set<File> getInProgressTasks() {
        return inProgressTasks;
    }

    synchronized private boolean alreadyCompleted(File file) {
        return completedTasks.contains(file);
    }

    synchronized private int getNumberOfCompletedTasks() {
        return completedTasks.size();
    }

    private int getTaskProgress() {
        return getNumberOfCompletedTasks() / totalTasks;
    }

    private boolean areAllTasksDone() {
        return getNumberOfCompletedTasks() == totalTasks;
    }
}
