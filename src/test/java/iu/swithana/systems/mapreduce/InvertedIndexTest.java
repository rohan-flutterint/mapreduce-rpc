package iu.swithana.systems.mapreduce;

import iu.swithana.systems.mapreduce.client.InvertedIndex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Before running this class, start a master and at least one worker.
 */
public class InvertedIndexTest extends InvertedIndex {
    @Before
    public void setup() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("test-config.properties").getFile());
        System.setProperty("config.file", file.getAbsolutePath());
    }

    @Test
    public void testInvertedIndexAccuracy() throws IOException {
        String result = runInvertedIndexProgram();
        String resultFile = result.substring(result.indexOf("[") + 1, result.indexOf("]"));
        final String[] actual = new String[1];
        try (Stream<String> stream = Files.lines(Paths.get(resultFile))) {
            stream.forEach(line -> {
                        if(line.startsWith("arm=")) {
                            actual[0] = line.substring(line.indexOf("=") + 1);
                        }
                    }
            );
        }
        String expected = "A_tale_of_two_cities.txt,the_romane_of_lust.txt";
        Assert.assertEquals(expected, actual[0]);
    }
}