package iu.swithana.systems.mapreduce.worker;

import iu.swithana.systems.mapreduce.core.Context;

import java.lang.reflect.InvocationTargetException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface WorkerRMI extends Remote {

    String printMessage(String name) throws RemoteException;

    String heartbeat() throws RemoteException;

    Context doMap(byte[] content, Class mapperClass) throws RemoteException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

    String doReduce(String key, Context context, Class reducerClass) throws RemoteException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;
}
