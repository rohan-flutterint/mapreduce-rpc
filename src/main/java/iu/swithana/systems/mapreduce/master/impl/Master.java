package iu.swithana.systems.mapreduce.master.impl;

import iu.swithana.systems.mapreduce.master.MasterRMI;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Master extends UnicastRemoteObject implements MasterRMI, Runnable {
    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(Master.class);
    private List<String> workers;
    private String ip;
    private int port;

    public Master(String ip, int port) throws RemoteException {
        super();
        this.workers = new ArrayList();
        this.ip = ip;
        this.port = port;
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
                Thread.sleep(10000);
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
}
