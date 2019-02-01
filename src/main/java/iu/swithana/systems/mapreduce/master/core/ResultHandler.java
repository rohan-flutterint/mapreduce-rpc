package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.core.Context;

import java.util.ArrayList;
import java.util.List;

public class ResultHandler {
    private volatile List<Context> resultsList;

    public ResultHandler() {
        this.resultsList = new ArrayList<>();
    }

    synchronized public void addResult(Context context) {
        resultsList.add(context);
    }

    public Context getResultsList() {
        Context context = new Context();
        for (Context jobContext : resultsList) {
            context.mergeContext(jobContext);
        }
        return context;
    }
}
