package iu.swithana.systems.mapreduce.master.core;

import java.util.Map;
import java.util.Set;

public interface ReducerResultListener {
    void onResult(Map<String, String> result, String workerID);

    void onError(Exception e, String workerID, String partition);
}
