package iu.swithana.systems.mapreduce.worker;

import iu.swithana.systems.mapreduce.master.MasterRMI;
import iu.swithana.systems.mapreduce.worker.impl.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    private static final int REGISTRY_PORT = 6666;
    private static final String IP = "localhost";

    public static void main(String[] args) {
        Registry lookupRegistry;
        try {
            lookupRegistry = LocateRegistry.getRegistry(REGISTRY_PORT);
            MasterRMI master = (MasterRMI) lookupRegistry.lookup("master");
            String workerID = master.registerWorker("localhost", 6666, "worker");
            logger.info("Registered the worker with the master, received the id: " + workerID);

            logger.info("Binding the worker with the registry " + workerID);
            Naming.rebind("//" + IP + ":" + REGISTRY_PORT + "/" + workerID, new Worker(workerID));
            logger.info("Worker " + workerID + " bounded with the registry");
        } catch (RemoteException e) {
            logger.error("Error connecting to the remote registry: " + e.getMessage(), e);
        } catch (MalformedURLException e) {
            logger.error("Error rebinding the worker: " + e.getMessage(), e);
        } catch (NotBoundException e) {
            logger.error("Error rebinding the worker: " + e.getMessage(), e);
        }
    }
}
