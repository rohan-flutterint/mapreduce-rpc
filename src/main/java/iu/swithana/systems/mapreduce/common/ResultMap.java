package iu.swithana.systems.mapreduce.common;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ResultMap implements Serializable {
    private Multimap<String, String> map;

    public ResultMap() {
        map = ArrayListMultimap.create();
    }

    public ResultMap(String key, ResultMap resultMap) {
        map = ArrayListMultimap.create();
        map.putAll(key, resultMap.getValues(key));
    }

    public void write(String key, String value) {
        map.put(key, value);
    }

    public Multimap<String, String> getMap() {
        return map;
    }

    private Collection<String> getValues(String key) {
        return map.get(key);
    }

    public Iterator<String> getIterator(String key) {
        return map.get(key).iterator();
    }

    public Set<String> getKeys() {
        return map.keySet();
    }

    synchronized public ResultMap getSubMap(String key, ResultMap resultMap) {
        ResultMap subContext = new ResultMap(key, resultMap);
        return subContext;
    }

    synchronized public ResultMap getSubMap(List<String> keys) {
        ResultMap subMap = new ResultMap();
        for (String key : keys) {
            subMap.putAll(key, this.map.get(key));
        }
        return subMap;
    }

    public void mergeContext(ResultMap resultMap) {
        this.map.putAll(resultMap.getMap());
    }

    private void putAll(String key, Iterable<String> values) {
        this.map.putAll(key, values);
    }
}
