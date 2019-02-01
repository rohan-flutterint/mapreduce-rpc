package iu.swithana.systems.mapreduce.master.core;
import iu.swithana.systems.mapreduce.core.Context;
import iu.swithana.systems.mapreduce.util.FileManager;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class MapperTask implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(MapperTask.class);
    private File file;
    private WorkerRMI worker;
    private FileManager fileManager;
    private Class mapperClass;
    private String workerID;
    private ResultListner resultListener;

    public MapperTask(File file, WorkerRMI worker, Class mapperClass, FileManager fileManager, String workerID,
                      ResultListner resultListener) {
        this.file = file;
        this.worker = worker;
        this.fileManager = fileManager;
        this.mapperClass = mapperClass;
        this.workerID = workerID;
        this.resultListener = resultListener;
    }

    @Override
    public void run() {
        try {
            Context context = worker.doMap(fileManager.readFile(this.file), mapperClass);
            resultListener.onResult(context, workerID);
        } catch (IOException e) {
            logger.error("Error accessing the file: " + file.getName() + " " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Exception occurred in completing the job on worker: " + this.workerID + " " + e.getMessage(), e);
            resultListener.onError(e, workerID);
        }
    }
}
