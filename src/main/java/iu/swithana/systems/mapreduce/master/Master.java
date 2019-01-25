package iu.swithana.systems.mapreduce.master;

import iu.swithana.systems.mapreduce.worker.MapperRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Master {
    private static Logger logger = LoggerFactory.getLogger(Master.class);
    public static Registry registry;

    private static final int REGISTRY_PORT = 6666;

    public static void main(String[] args) {

        try {
            // Start the registry
            startRegistry(REGISTRY_PORT);

            // spawn the workers
            Thread.sleep(5000);

            // submit a test job
            testJob(REGISTRY_PORT);

        } catch (RemoteException e) {
            logger.error("Error occurred while accessing the registry: "+ e.getMessage(), e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void testJob(int port) {
        Registry  lookupRegistry= null;
        try {
            lookupRegistry = LocateRegistry.getRegistry(port);
            MapperRMI mapper = (MapperRMI) lookupRegistry.lookup("map");
            String result = mapper.printMessage("Sachith");
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
