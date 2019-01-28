package iu.swithana.systems.mapreduce.worker;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface WorkerRMI extends Remote {

    String printMessage(String name) throws RemoteException;

    String heartbeat() throws RemoteException;
}
