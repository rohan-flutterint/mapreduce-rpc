package iu.swithana.systems.mapreduce.master.core;

import java.util.*;

public class ReducerResultsHandler {
    private volatile Map<String, String> results;

    public ReducerResultsHandler() {
        this.results = new TreeMap<>();
    }

    synchronized public void addResult(String key, String value) {
        results.put(key, value);
    }

    public Map<String, String> getAllResults() {
        return results;
    }
}
