package iu.swithana.systems.mapreduce.worker.impl;

import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class Worker extends UnicastRemoteObject implements WorkerRMI {

    private static Logger logger = LoggerFactory.getLogger(Worker.class);
    private static final long serialVersionUID = 1L;

    private String id;

    public Worker(String id) throws RemoteException {
        super();
        this.id = id;
    }

    public String printMessage(String name) {
        logger.info("Message received from: " + name);
        return "Hello " + name;
    }

    public String heartbeat() {
        return id;
    }
}
