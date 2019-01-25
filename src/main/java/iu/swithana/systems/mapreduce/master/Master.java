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

    private Master() {
    }

    public static void main(String[] args) {
        try {
            Registry registry = LocateRegistry.getRegistry(6666);
            MapperRMI stub = (MapperRMI) registry.lookup("map");
            stub.printMessage("Sachith");
            logger.info("Invoked the worker!");
        } catch (AccessException e) {
            logger.error("Error accessing the registry: " + e.getMessage(), e);
        } catch (RemoteException e) {
            logger.error("Error occurred while accessing the registry: "+ e.getMessage(), e);
        } catch (NotBoundException e) {
            logger.error("Error occurred while retrieving RPC bind: "+ e.getMessage(), e);
        }
    }
}
