package iu.swithana.systems.mapreduce.core;

public interface Mapper {
    void map(String input, ResultMap resultMap, JobContext jobContext);
}
