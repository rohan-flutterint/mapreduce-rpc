package iu.swithana.systems.mapreduce.worker;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MapperRMI extends Remote {

    String printMessage(String name) throws RemoteException;

}
