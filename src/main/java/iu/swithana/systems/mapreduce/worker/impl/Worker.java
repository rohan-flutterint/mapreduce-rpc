package iu.swithana.systems.mapreduce.worker.impl;

import com.google.common.base.Charsets;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
        writeToStore(resultMap, (String) jobConfigs.getConfig("jobid"),
                (String) jobConfigs.getConfig("filename"));
        logger.info("[" + id + "] Completed a map job");
        logger.debug("Writing the results to the store: ...");
        return resultMap;
    }

    public Map<String, String> doReduce(String partition, String jobID, Class reducerClass) throws RemoteException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor constructor = reducerClass.getConstructor();
        Reducer reducer = (Reducer) constructor.newInstance();
        ResultMap partitionedValues = getValuesForPartition(partition, jobID);
        Map<String, String> results = new HashMap<>();
        for (String key : partitionedValues.getKeys()) {
            results.put(key, reducer.reduce(key, partitionedValues.getIterator(key)));
        }
        logger.info("[" + id + "] Completed a reduce job for job: " + jobID + " and the partition: " + partition);
        return results;
    }

    private void writeToStore(ResultMap results, String jobId, String filename) {
        Map<String, String> resultAsStrings = results.getResultAsStringList();
        for (String key : resultAsStrings.keySet()) {
            int partition = Math.abs(key.hashCode() % partitionNumber);
            putToStore(jobId + "/" + partition + "/" + id + "/" + filename + "/" + key, resultAsStrings.get(key)
                    .replace("[", "").replace("]", ""));
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

    private ResultMap getValuesForPartition(String partition, String jobID) {
        ResultMap resultMap = new ResultMap();
        String prefixKey = jobID + "/" + partition;
        ByteSequence keySeq = ByteSequence.from(prefixKey, Charsets.UTF_8);
        CompletableFuture<GetResponse> responseFuture = kvClient.get(keySeq, GetOption.newBuilder().withPrefix(keySeq).build());
        try {
            while (responseFuture.isDone()) {
                Thread.sleep(3000);
            }
            List<KeyValue> keyValues = responseFuture.get().getKvs();
            for (KeyValue keyValue : keyValues) {
                String fullkey = keyValue.getKey().toString(Charsets.UTF_8);
                String key = fullkey.substring(fullkey.lastIndexOf("/") + 1);
                String values = keyValue.getValue().toString(Charsets.UTF_8).replaceAll("\\s", "");
                for (String value : values.split(",")) {
                    resultMap.write(key, value);
                }
            }
        } catch (Exception e) {
            logger.error("Error occurred while getting values from the key value store: " + e.getMessage(), e);
        }
        return resultMap;
    }
}
