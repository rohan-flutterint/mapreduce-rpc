package iu.swithana.systems.mapreduce.master;

import iu.swithana.systems.mapreduce.config.Config;
import iu.swithana.systems.mapreduce.config.Constants;
import iu.swithana.systems.mapreduce.master.impl.Master;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);
    public static Registry registry;

    private static int REGISTRY_PORT;
    private static int HEARTBEAT_TIMEOUT;
    private static String REGISTRY_HOST;
    private static String RMI_MASTER;

    public static void main(String[] args) {
        try {
            // loading the configs
            Config config = new Config();
            REGISTRY_PORT = Integer.parseInt(config.getConfig(Constants.RMI_REGISTRY_PORT));
            REGISTRY_HOST = config.getConfig(Constants.RMI_REGISTRY_HOST);
            HEARTBEAT_TIMEOUT = Integer.parseInt(config.getConfig(Constants.HEARTBEAT_TIMEOUT));
            RMI_MASTER = config.getConfig(Constants.RMI_MASTER);

            // Start the registry
            startRegistry(REGISTRY_PORT);

            // Starting the master
            Master master = new Master(REGISTRY_HOST, REGISTRY_PORT, HEARTBEAT_TIMEOUT);
            Naming.bind("//"+ REGISTRY_HOST + ":" + REGISTRY_PORT + "/" + RMI_MASTER, master);
            logger.info("Mapper bound");
            logger.info("Master ready to accept workers");

            // waiting for the workers to spawn
            Thread.sleep(10000);

            String workerName = "";
            List<String> workers = master.getWorkers();
            if(!workers.isEmpty()) {
                workerName = workers.get(0);
            }

            Thread masterThread = new Thread(master);
            masterThread.start();

            // submit a test job
            testJob(workerName, REGISTRY_PORT);
        } catch (RemoteException e) {
            logger.error("Error occurred while accessing the registry: "+ e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error("Error occurred while accessing the registry: "+ e.getMessage(), e);
        } catch (AlreadyBoundException e) {
            logger.error("Error occurred while binding the master to the registry: "+ e.getMessage(), e);
        } catch (MalformedURLException e) {
            logger.error("Error occurred while binding the master to the registry: "+ e.getMessage(), e);
        } catch (IOException e) {
            logger.error("Error accessing the configuration file: "+ e.getMessage(), e);
        }
    }

    private static void testJob(String workerID, int port) {
        if (workerID == "") {
            System.out.println("The workerlist is empty!");
        }
        Registry  lookupRegistry;
        try {
            lookupRegistry = LocateRegistry.getRegistry(port);
            WorkerRMI mapper = (WorkerRMI) lookupRegistry.lookup(workerID);
            String result = mapper.getWorkerID();
            logger.info("Invoked the worker!");
            logger.info("Result: " + result);
        } catch (AccessException e) {
            logger.error("Error accessing the registry: " + e.getMessage(), e);
        } catch (RemoteException e) {
            logger.error("Error occurred while accessing the registry: "+ e.getMessage(), e);
        } catch (NotBoundException e) {
            logger.error("Error occurred while retrieving RPC bind: "+ e.getMessage(), e);
        }
    }

    private static void startRegistry(int port) throws RemoteException{
        try {
            // check if a registry already exists at the port
            registry = LocateRegistry.getRegistry(port);
            registry.list();
        } catch (RemoteException e) {
            // create registry if one is not found
            logger.info("A registry cannot be found at port: " + port);
            registry = LocateRegistry.createRegistry(port);
            logger.info("Created a new registry at port:  " + port);
        }
    }
}
