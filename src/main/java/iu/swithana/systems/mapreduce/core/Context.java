package iu.swithana.systems.mapreduce.core;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class Context implements Serializable {
    private Multimap<String, String> map;

    public Context() {
        map = ArrayListMultimap.create();
    }

    public Context(String key, Context context) {
        map = ArrayListMultimap.create();
        map.putAll(key, context.getValues(key));
    }

    public void write(String key, String value) {
        map.put(key, value);
    }

    @Override
    public String toString() {
        return "Context{" +
                "results=" + map +
                '}';
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

    public Context getSubContext(String key, Context context) {
        Context subContext = new Context(key, context);
        return subContext;
    }
}
