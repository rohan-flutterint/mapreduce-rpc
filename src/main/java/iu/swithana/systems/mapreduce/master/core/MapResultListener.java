package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.common.ResultMap;

import java.io.File;

public interface MapResultListener {
    void onResult(ResultMap resultMap, String workerID);

    void onError(Exception e, String workerID, File file);
}
