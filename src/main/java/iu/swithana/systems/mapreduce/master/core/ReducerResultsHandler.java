package iu.swithana.systems.mapreduce.master.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReducerResultsHandler {
    private volatile Map<String, String> results;

    public ReducerResultsHandler() {
        this.results = new HashMap<>();
    }

    synchronized public void addResult(String key, String value) {
        results.put(key, value);
    }

    public Map<String, String> getAllResults() {
        return results;
    }
}
