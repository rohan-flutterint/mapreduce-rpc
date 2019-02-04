package iu.swithana.systems.mapreduce.master;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MapRedRMI extends Remote {
    String submitJob(Class mapperClass, Class reducerClass, String inputDirectory, String outputDirectory) throws RemoteException;
}
