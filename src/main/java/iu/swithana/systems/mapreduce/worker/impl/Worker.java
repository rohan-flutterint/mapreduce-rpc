package iu.swithana.systems.mapreduce.worker.impl;

import iu.swithana.systems.mapreduce.worker.MapperRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.rmi.AccessException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Worker extends UnicastRemoteObject implements MapperRMI {

    private static Logger logger = LoggerFactory.getLogger(Worker.class);
    private static final long serialVersionUID = 1L;

    protected Worker() throws RemoteException {
    }

    public String printMessage(String name) {
        logger.info("Message received from: " + name);
        return "Hello " + name;
    }

    public static void main(String[] args) {
        try {
            Naming.rebind("//localhost:6666/map", new Worker());
            logger.info("Mapper bound");
            logger.info("Server ready");
        } catch (RemoteException e) {
            logger.error("Error connecting to the remote registry: " + e.getMessage(), e);
        } catch (MalformedURLException e) {
            logger.error("Error rebinding the mapper: " + e.getMessage(), e);
        }
    }
}
