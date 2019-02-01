package iu.swithana.systems.mapreduce.common;

import java.util.Iterator;

public interface Reducer {
    String reduce(String key, Iterator<String> values);
}
