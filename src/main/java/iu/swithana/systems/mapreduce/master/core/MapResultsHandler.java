package iu.swithana.systems.mapreduce.master.core;

import iu.swithana.systems.mapreduce.common.ResultMap;

import java.util.ArrayList;
import java.util.List;

public class MapResultsHandler {
    private volatile List<ResultMap> resultsList;

    public MapResultsHandler() {
        this.resultsList = new ArrayList<>();
    }

    synchronized public void addResult(ResultMap resultMap) {
        resultsList.add(resultMap);
    }

    public ResultMap getResultsList() {
        ResultMap resultMap = new ResultMap();
        for (ResultMap jobContext : resultsList) {
            resultMap.mergeContext(jobContext);
        }
        return resultMap;
    }
}
