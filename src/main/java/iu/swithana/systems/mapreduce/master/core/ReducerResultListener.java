package iu.swithana.systems.mapreduce.master.core;

public interface ReducerResultListener {
    void onResult(String result, String key, String workerID);

    void onError(Exception e, String workerID, String key);
}
