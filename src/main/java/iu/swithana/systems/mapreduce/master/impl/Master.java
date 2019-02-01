package iu.swithana.systems.mapreduce.master.impl;

import iu.swithana.systems.mapreduce.core.Context;
import iu.swithana.systems.mapreduce.master.MapRedRMI;
import iu.swithana.systems.mapreduce.master.MasterRMI;
import iu.swithana.systems.mapreduce.util.FileManager;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Master extends UnicastRemoteObject implements MasterRMI, Runnable, MapRedRMI {
    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(Master.class);
    private List<String> workers;
    private String ip;
    private int port;
    private int heartbeatTimeout;

    public Master(String ip, int port, int heartbeatTimeout) throws RemoteException {
        super();
        this.workers = new ArrayList();
        this.ip = ip;
        this.port = port;
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public String registerWorker(String ip, int port, String name) {
        UUID uuid = UUID.randomUUID();
        String id = uuid.toString();
        workers.add(id);
        logger.info("Registered new worker: " + id);
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
     * @return a list of unresponsive worker node IDs.
     */
    private List<String> testHeatbeat() {
        Registry lookupRegistry = null;
        try {
            lookupRegistry = LocateRegistry.getRegistry(port);
        } catch (RemoteException e) {
            logger.error("Error occurred while locating the registry: "+ e.getMessage(), e);
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

    public String submitJob(Class mapperClass, Class reducerClass, String inputDirectory) {
        WorkerRMI worker = getWorker(workers.get(0));
        Map<String, String> result = new HashMap();
        FileManager fileManager = new FileManager();
        File[] filesInDirectory = fileManager.getFilesInDirectory(inputDirectory);
        try {
            if (worker != null) {
                byte[] fileContent = fileManager.readFile(filesInDirectory[0]);
                Context context = worker.doMap(fileContent, mapperClass);
                for (String key : context.getKeys()) {
                    Context subContext = context.getSubContext(key, context);
                    result.put(key, worker.doReduce(key, subContext, reducerClass));
                }
                return result.toString();
            }
        } catch (RemoteException e) {
            logger.error("Error accessing Worker: " + e.getMessage(), e);
        } catch (IOException e) {
            logger.error("Error accessing input file: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error accessing the reducer function: " + e.getMessage(), e);
        }
        return "Hello Client, got the classes: " + mapperClass.getSimpleName() + "  " + reducerClass.getSimpleName();
    }

    private WorkerRMI getWorker(String workerID) {
        Registry  lookupRegistry;
        WorkerRMI worker = null;
        try {
            lookupRegistry = LocateRegistry.getRegistry(port);
            worker = (WorkerRMI) lookupRegistry.lookup(workerID);
            logger.info("Invoked the worker!");
        } catch (AccessException e) {
            logger.error("Error accessing the registry: " + e.getMessage(), e);
        } catch (RemoteException e) {
            logger.error("Error occurred while accessing the registry: "+ e.getMessage(), e);
        } catch (NotBoundException e) {
            logger.error("Error occurred while retrieving RPC bind: "+ e.getMessage(), e);
        }
        return worker;
    }
}
