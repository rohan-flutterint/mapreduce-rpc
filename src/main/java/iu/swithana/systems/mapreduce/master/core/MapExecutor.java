package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.core.ResultMap;
import iu.swithana.systems.mapreduce.util.FileManager;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.*;

public class MapExecutor {
    private static Logger logger = LoggerFactory.getLogger(MapExecutor.class);

    private Class mapperClass;
    private String inputDirectory;
    private Hashtable<String, WorkerRMI> workerTable;
    private BlockingQueue<String> idleWorkers;
    private int numbeOfWorkers;
    private volatile List<File> fileList;

    public MapExecutor(Class mapperClass, String inputDirectory, Hashtable<String, WorkerRMI> workerTable) {
        this.workerTable = workerTable;
        this.mapperClass = mapperClass;
        this.inputDirectory = inputDirectory;
        this.idleWorkers = new ArrayBlockingQueue<>(workerTable.keySet().size());
        this.numbeOfWorkers = workerTable.keySet().size();
        this.fileList = Collections.synchronizedList(new ArrayList<File>());
    }

    public ResultMap runJob() {
        // prepare the input files list
        FileManager fileManager = new FileManager();
        fileList.addAll(fileManager.getFilesInDirectory(inputDirectory));
        final ResultHandler resultHandler = new ResultHandler();

        // add all the workers to the idle queue
        for (String workerID : workerTable.keySet()) {
            idleWorkers.add(workerID);
        }

        // run the thread pool, each per worker
        try {
            ExecutorService executor = Executors.newFixedThreadPool(numbeOfWorkers);
            for (File file : fileList) {
                if (file.isFile()) {
                    // blocks on the idle queue, waits for an available worker
                    final String workerID = idleWorkers.take();
                    WorkerRMI worker = workerTable.get(workerID);
                    logger.debug("Submitting file " + file.getName() + " to worker: " + workerID);
                    executor.submit(new MapperTask(file, worker, mapperClass, fileManager,
                            worker.getWorkerID(), new ResultListner() {
                            // in case of successful execution, save the result
                            @Override
                            public void onResult(ResultMap resultMap, String workerID) {
                                resultHandler.addResult(resultMap);
                                idleWorkers.add(workerID);
                            }

                            // if the execution fails, retry for the same file
                            @Override
                            public void onError(Exception e, String workerID, File file) {
                                logger.error("Error accessing Worker: " + workerID +
                                        ". Assuming it's inaccessible and dropping the worker. " + e.getMessage(), e);
                                logger.error("Adding file: " + file.getName() + " to the fileList.");
                                addToRetryList(file);
                            }
                        }));
                }
            }
            // wait till all the threads have been completed, then cleanup.
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);

        } catch (RemoteException e) {
            logger.error("Error accessing Worker: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error accessing the mapper function: " + e.getMessage(), e);
        }
        return resultHandler.getResultsList();
    }

    synchronized private void addToRetryList(File file) {
        this.fileList.add(file);
    }
}
