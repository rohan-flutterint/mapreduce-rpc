package iu.swithana.systems.mapreduce.worker;

import iu.swithana.systems.mapreduce.common.ResultMap;
import iu.swithana.systems.mapreduce.common.JobContext;

import java.lang.reflect.InvocationTargetException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface WorkerRMI extends Remote {

    String getWorkerID() throws RemoteException;

    String heartbeat() throws RemoteException;

    ResultMap doMap(byte[] content, Class mapperClass, JobContext configs) throws RemoteException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;

    Map<String, String> doReduce(String partition, String jobID, Class reducerClass) throws RemoteException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException;
}
