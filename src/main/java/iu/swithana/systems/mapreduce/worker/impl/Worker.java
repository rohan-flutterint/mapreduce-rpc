package iu.swithana.systems.mapreduce.worker.impl;

import iu.swithana.systems.mapreduce.common.ResultMap;
import iu.swithana.systems.mapreduce.common.JobContext;
import iu.swithana.systems.mapreduce.common.Mapper;
import iu.swithana.systems.mapreduce.common.Reducer;
import iu.swithana.systems.mapreduce.worker.WorkerRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class Worker extends UnicastRemoteObject implements WorkerRMI {

    private static Logger logger = LoggerFactory.getLogger(Worker.class);
    private static final long serialVersionUID = 1L;

    private String id;

    public Worker(String id) throws RemoteException {
        super();
        this.id = id;
    }

    public String getWorkerID() {
        return this.id;
    }

    public String heartbeat() {
        return this.id;
    }

    public ResultMap doMap(byte[] content, Class mapperClass, JobContext jobConfigs) throws RemoteException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        ResultMap resultMap = new ResultMap();
        Constructor constructor = mapperClass.getConstructor();
        Mapper mapper = (Mapper) constructor.newInstance();
        mapper.map(new String(content), resultMap, jobConfigs);
        logger.debug("[" + id + "] Completed a map job");
        return resultMap;
    }

    public String doReduce(String key, ResultMap resultMap, Class reducerClass) throws RemoteException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor constructor = reducerClass.getConstructor();
        Reducer reducer = (Reducer) constructor.newInstance();
        logger.debug("[" + id + "] Completed a reduce job for the key: " + key);
        return reducer.reduce(key, resultMap.getIterator(key));
    }
}
