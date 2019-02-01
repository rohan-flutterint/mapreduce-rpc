package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.core.Context;

public interface ResultListner {
    void onResult(Context context, String workerID);

    void onError(Exception e, String workerID);
}
