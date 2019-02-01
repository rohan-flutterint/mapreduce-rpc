package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.core.ResultMap;

import java.io.File;

public interface ResultListner {
    void onResult(ResultMap resultMap, String workerID);

    void onError(Exception e, String workerID, File file);
}
