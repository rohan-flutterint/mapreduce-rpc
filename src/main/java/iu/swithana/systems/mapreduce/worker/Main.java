package iu.swithana.systems.mapreduce.worker;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import iu.swithana.systems.mapreduce.config.Config;
import iu.swithana.systems.mapreduce.config.Constants;
import iu.swithana.systems.mapreduce.master.MasterRMI;
import iu.swithana.systems.mapreduce.worker.impl.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    private static int REGISTRY_PORT;
    private static int PARTITION_NUMBER;
    private static String REGISTRY_HOST;
    private static String RMI_MASTER;
    private static String KEYVAL_STORE_HOST;
    private static String KEYVAL_STORE_PORT;

    public static void main(String[] args) {
        Registry lookupRegistry;
        try {
            // loading the configs
            Config config = new Config();
            REGISTRY_PORT = Integer.parseInt(config.getConfig(Constants.RMI_REGISTRY_PORT));
            PARTITION_NUMBER = Integer.parseInt(config.getConfig(Constants.PARTITION_NUMBER));
            REGISTRY_HOST = config.getConfig(Constants.RMI_REGISTRY_HOST);
            RMI_MASTER = config.getConfig(Constants.RMI_MASTER);
            KEYVAL_STORE_HOST = config.getConfig(Constants.KEYVAL_STORE_HOST);
            KEYVAL_STORE_PORT = config.getConfig(Constants.KEYVAL_STORE_PORT);

            lookupRegistry = LocateRegistry.getRegistry(REGISTRY_PORT);
            MasterRMI master = (MasterRMI) lookupRegistry.lookup(RMI_MASTER);
            String workerID = master.registerWorker(REGISTRY_HOST, REGISTRY_PORT, "worker");
            logger.info("Registered the worker with the master, received the id: " + workerID);

            Client client = Client.builder().endpoints("http://localhost:2379").build();
            KV kvClient = client.getKVClient();

            logger.info("Binding the worker with the registry " + workerID);
            Naming.rebind("//" + REGISTRY_HOST + ":" + REGISTRY_PORT + "/" + workerID,
                    new Worker(workerID, KEYVAL_STORE_HOST, KEYVAL_STORE_PORT, PARTITION_NUMBER, kvClient));
            logger.info("Worker " + workerID + " bounded with the registry");
        } catch (RemoteException e) {
            logger.error("Error connecting to the remote registry: " + e.getMessage(), e);
        } catch (MalformedURLException e) {
            logger.error("Error rebinding the worker: " + e.getMessage(), e);
        } catch (NotBoundException e) {
            logger.error("Error rebinding the worker: " + e.getMessage(), e);
        } catch (IOException e) {
            logger.error("Error accessing the configuration file: "+ e.getMessage(), e);
        }
    }
}
