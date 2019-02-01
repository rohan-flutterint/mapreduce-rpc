package iu.swithana.systems.mapreduce.common;

public interface Mapper {
    void map(String input, ResultMap resultMap, JobContext jobContext);
}
