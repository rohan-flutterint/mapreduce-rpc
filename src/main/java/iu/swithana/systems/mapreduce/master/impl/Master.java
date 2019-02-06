package iu.swithana.systems.mapreduce.master.impl;

import com.google.common.io.Files;
import iu.swithana.systems.mapreduce.common.ResultMap;
import iu.swithana.systems.mapreduce.master.MapRedRMI;
import iu.swithana.systems.mapreduce.master.MasterRMI;
import iu.swithana.systems.mapreduce.master.core.MapExecutor;
import iu.swithana.systems.mapreduce.master.core.ReducerExecutor;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.*;

public class Master extends UnicastRemoteObject implements MasterRMI, Runnable, MapRedRMI {
    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(Master.class);
    private List<String> workers;
    private String ip;
    private int port;
    private int heartbeatTimeout;
    private int partitionSize;
    private Hashtable<String, WorkerRMI> workerTable;

    public Master(String ip, int port, int heartbeatTimeout, int partitionSize) throws RemoteException {
        super();
        this.workers = new ArrayList();
        this.ip = ip;
        this.port = port;
        this.heartbeatTimeout = heartbeatTimeout;
        this.partitionSize = partitionSize;
    }

    public String registerWorker(String ip, int port, String name) {
        UUID uuid = UUID.randomUUID();
        String id = uuid.toString();
        logger.info("Registered new worker: " + id);
        workers.add(id);
        return id;
    }

    public List<String> getWorkers() {
        return workers;
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(heartbeatTimeout);
                workerMonitor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private void workerMonitor() {
        List<String> stoppedWorkers = testHeatbeat();
        for (String worker : stoppedWorkers) {
            logger.info("Cleaning up the worker due to unresponsiveness " + worker);
            workers.remove(worker);
        }
    }

    /**
     * Detects unresponsive worker nodes.
     *
     * @return a list of unresponsive worker node IDs.
     */
    private List<String> testHeatbeat() {
        Registry lookupRegistry = null;
        try {
            lookupRegistry = LocateRegistry.getRegistry(port);
        } catch (RemoteException e) {
            logger.error("Error occurred while locating the registry: " + e.getMessage(), e);
        }
        if (lookupRegistry == null) {
            logger.error("Heartbeats cannot be checked, the registry is not working!");
            return null;
        }

        List<String> stoppedWorkers = new ArrayList();
        for (String workerID : workers) {
            try {
                WorkerRMI worker = (WorkerRMI) lookupRegistry.lookup(workerID);
                String result = worker.heartbeat();
                if (result.equals(workerID)) {
                    logger.debug(workerID + " is responsive");
                } else {
                    logger.error(workerID + " is not responding!");
                    stoppedWorkers.add(workerID);
                }
            } catch (Exception e) {
                logger.error(workerID + " is not responding! " + e.getMessage());
                stoppedWorkers.add(workerID);
            }
        }
        return stoppedWorkers;
    }

    public String submitJob(Class mapperClass, Class reducerClass, String inputDirectory, String outputDirectory) {
        // update the list with the workers
        int registerWorkers = registerWorkers();
        // create jobID
        String dateID = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String outputFilePath = outputDirectory + File.separator + "app_" + dateID + File.separator + "output";

        // no registered workers available
        if (registerWorkers < 1) {
            return "No workers are available. Please start a worker before submitting a mapreduce job";
        }
        // schedule the map job
        MapExecutor mapExecutor = new MapExecutor(mapperClass, inputDirectory, workerTable);
        final Map<String, String> result;
        try {
            ResultMap resultMap = mapExecutor.runJob();
            ReducerExecutor reducerExecutor = new ReducerExecutor(resultMap, workerTable, reducerClass, partitionSize);
            result = reducerExecutor.runJob();

            // create output directory and write the file
            Files.createParentDirs(new File(outputFilePath));
            java.nio.file.Files.write(Paths.get(outputFilePath), () -> result.entrySet().stream()
                    .<CharSequence>map(e -> e.getKey() + "=" + e.getValue())
                    .iterator());

            return "The MapReduce job successful! The results are at: [" + outputFilePath + "]";
        } catch (Exception e) {
            logger.error("Error running the MapReduce job: " + e.getMessage(), e);
        }
        return "Job failed!, Please resubmit the job.";
    }

    private WorkerRMI getWorker(String workerID) {
        Registry lookupRegistry;
        WorkerRMI worker = null;
        try {
            lookupRegistry = LocateRegistry.getRegistry(port);
            worker = (WorkerRMI) lookupRegistry.lookup(workerID);
            logger.info("Invoked the worker!");
        } catch (AccessException e) {
            logger.error("Error accessing the registry: " + e.getMessage(), e);
        } catch (RemoteException e) {
            logger.error("Error occurred while accessing the registry: " + e.getMessage(), e);
        } catch (NotBoundException e) {
            logger.error("Error occurred while retrieving RPC bind: " + e.getMessage(), e);
        }
        return worker;
    }

    private int registerWorkers() {
        workerTable = new Hashtable<>();
        Registry lookupRegistry;
        for (String workerID : workers) {
            try {
                lookupRegistry = LocateRegistry.getRegistry(port);
                workerTable.put(workerID, (WorkerRMI) lookupRegistry.lookup(workerID));
            } catch (RemoteException e) {
                logger.error("Error accessing Worker: " + e.getMessage(), e);
            } catch (NotBoundException e) {
                logger.error("Cannot register worker: " + workerID + "  " + e.getMessage(), e);
            }
        }
        return workerTable.keySet().size();
    }
}
