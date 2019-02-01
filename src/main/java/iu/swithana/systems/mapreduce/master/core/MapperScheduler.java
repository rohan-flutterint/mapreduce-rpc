package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.core.Context;
import iu.swithana.systems.mapreduce.util.FileManager;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.concurrent.*;

public class MapperScheduler {
    private static Logger logger = LoggerFactory.getLogger(MapperScheduler.class);

    private Class mapperClass;
    private String inputDirectory;
    private Hashtable<String, WorkerRMI> workerTable;
    private BlockingQueue<String> idleWorkers;
    private int numbeOfWorkers;

    public MapperScheduler(Class mapperClass, String inputDirectory, Hashtable<String, WorkerRMI> workerTable) {
        this.workerTable = workerTable;
        this.mapperClass = mapperClass;
        this.inputDirectory = inputDirectory;
        this.idleWorkers = new ArrayBlockingQueue<>(workerTable.keySet().size());
        this.numbeOfWorkers = workerTable.keySet().size();
    }

    public Context runJob() {
        // prepare the input files list
        FileManager fileManager = new FileManager();
        File[] filesInDirectory = fileManager.getFilesInDirectory(inputDirectory);
        final ResultHandler resultHandler = new ResultHandler();

        // add all the workers to the idle queue
        for (String workerID : workerTable.keySet()) {
            idleWorkers.add(workerID);
        }

        // run the thread pool, each per worker
        try {
            ExecutorService executor = Executors.newFixedThreadPool(numbeOfWorkers);
            for (int i = 0; i < filesInDirectory.length; i++) {
                if (filesInDirectory[i].isFile()) {
                    // blocks on the idle queue, waits for an available worker
                    final String workerID = idleWorkers.take();
                    WorkerRMI worker = workerTable.get(workerID);
                    logger.debug("Submitting file " + filesInDirectory[i].getName() + " to worker: " + workerID);
                    executor.submit(new MapperTask(filesInDirectory[i], worker, mapperClass, fileManager,
                            worker.getWorkerID(), new ResultListner() {
                            @Override
                            public void onResult(Context context, String workerID) {
                                resultHandler.addResult(context);
                                idleWorkers.add(workerID);
                            }

                            @Override
                            public void onError(Exception e, String workerID) {
                                logger.error("Error accessing Worker: " + workerID + ". Assuming it's inaccessible. "
                                        + e.getMessage(), e);
                            }
                        }));
                }
            }
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);

        } catch (RemoteException e) {
            logger.error("Error accessing Worker: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error accessing the mapper function: " + e.getMessage(), e);
        }
        return resultHandler.getResultsList();
    }
}
