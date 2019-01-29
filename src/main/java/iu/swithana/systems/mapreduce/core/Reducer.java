package iu.swithana.systems.mapreduce.core;

import java.util.Iterator;

public interface Reducer {
    String reduce(String key, Iterator<String> values);
}
