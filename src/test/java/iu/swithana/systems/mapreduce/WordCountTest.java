package iu.swithana.systems.mapreduce;

import iu.swithana.systems.mapreduce.client.WordCount;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Before running this class, start a master and at least one worker.
 */
public class WordCountTest extends WordCount {

    @Test
    public void testWordCountAccuracy() throws IOException {
        String result = runWordCountProgram();
        String resultFile = result.substring(result.indexOf("[") + 1, result.indexOf("]"));
        final String[] actual = new String[1];
        try (Stream<String> stream = Files.lines(Paths.get(resultFile))) {
            stream.forEach(line -> {
                        if(line.startsWith("pride=")) {
                            actual[0] = line.substring(line.indexOf("=") + 1);
                        }
                    }
            );
        }
        Assert.assertEquals("45", actual[0]);
    }

}
