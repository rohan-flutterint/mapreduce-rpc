package iu.swithana.systems.mapreduce.core;

import java.io.Serializable;
import java.util.Hashtable;

public class JobContext implements Serializable {
    private Hashtable<String, Object> jobConfigs;

    public JobContext() {
        this.jobConfigs = new Hashtable<>();
    }

    public void addConfig(String key, Object object) {
        jobConfigs.put(key, object);
    }

    public Object getConfig(String key) {
        return jobConfigs.get(key);
    }
}
