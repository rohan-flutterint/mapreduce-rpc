package iu.swithana.systems.mapreduce.worker.impl;

import com.google.common.base.Charsets;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Worker extends UnicastRemoteObject implements WorkerRMI {

    private static Logger logger = LoggerFactory.getLogger(Worker.class);
    private static final long serialVersionUID = 1L;

    private String id;
    private String storeHost;
    private String storePort;
    private int partitionNumber;
    private KV kvClient;

    public Worker(String id, String storeHost, String storePort, int partitionNumber, KV kvClient) throws RemoteException {
        super();
        this.id = id;
        this.storeHost = storeHost;
        this.storePort = storePort;
        this.partitionNumber = partitionNumber;
        this.kvClient = kvClient;
    }

    public String getWorkerID() {
        return this.id;
    }

    public String heartbeat() {
        return this.id;
    }

    public ResultMap doMap(byte[] content, Class mapperClass, JobContext jobConfigs) throws RemoteException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        logger.info("[" + id + "] Accepted a map job for file: " + jobConfigs.getConfig("filename"));
        ResultMap resultMap = new ResultMap();
        Constructor constructor = mapperClass.getConstructor();
        Mapper mapper = (Mapper) constructor.newInstance();
        mapper.map(new String(content), resultMap, jobConfigs);
        writeToStore(resultMap, partitionNumber,
                (String) jobConfigs.getConfig("jobid"), (String) jobConfigs.getConfig("filename"));
        logger.info("[" + id + "] Completed a map job");
        logger.debug("Writing the results to the store: ...");
        return resultMap;
    }

    public Map<String, String> doReduce(int partitionNumber, String JobID, Class reducerClass) throws RemoteException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor constructor = reducerClass.getConstructor();
        Reducer reducer = (Reducer) constructor.newInstance();
        Map<String, String> results = new HashMap<>();
        return results;
    }


    /**
     * This method is deprecated!!!
     * @param resultMap
     * @param reducerClass
     * @return
     * @throws RemoteException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public Map<String, String> doReduce(ResultMap resultMap, Class reducerClass) throws RemoteException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor constructor = reducerClass.getConstructor();
        Reducer reducer = (Reducer) constructor.newInstance();
        Map<String, String> results = new HashMap<>();
        for (String key : resultMap.getKeys()) {
            results.put(key, reducer.reduce(key, resultMap.getIterator(key)));
        }
        logger.debug("[" + id + "] Completed a reduce job for the keys: " + resultMap.getKeys());
        return results;
    }

    private void writeToStore(ResultMap results, int partitions, String jobId, String filename) {
        Map<String, String> resultAsStrings = results.getResultAsStringList();
        for (String key : resultAsStrings.keySet()) {
            int partition = Math.abs(key.hashCode() % partitionNumber);
            putToStore(jobId + "/" + id + "/" + partition + "/" + filename + "/" + key, resultAsStrings.get(key));
        }
    }

    private void putToStore(String key, String value) {
        ByteSequence keySeq = ByteSequence.from(key, Charsets.UTF_8);
        ByteSequence valueSeq = ByteSequence.from(value, Charsets.UTF_8);
        try {
            kvClient.put(keySeq, valueSeq).get();
        } catch (Exception e) {
            logger.error("Cannot connect to the key value store: " + e.getMessage(), e);
        }
    }
}
