package iu.swithana.systems.mapreduce.master;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MasterRMI extends Remote {
    /**
     * Allows workers to register with the master.
     * @param ip
     * @param port
     * @return the worker_id
     */
    String registerWorker(String ip, int port, String name) throws RemoteException;
}
