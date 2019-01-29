package iu.swithana.systems.mapreduce.client;

import iu.swithana.systems.mapreduce.core.Context;
import iu.swithana.systems.mapreduce.core.Mapper;
import iu.swithana.systems.mapreduce.core.Reducer;
import iu.swithana.systems.mapreduce.master.MapRedRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;

public class WordCount {
    private static Logger logger = LoggerFactory.getLogger(WordCount.class);

    private static final int REGISTRY_PORT = 6666;
    private static final String IP = "localhost";
    private static final String MASTER_LOOKUP = "master";
    private static final String INPUT_DIR = "/Users/swithana/git/mapreduce-rpc/input/WordCountTest.txt";

    public static void main(String[] args) {
        Registry lookupRegistry;
        try {
            lookupRegistry = LocateRegistry.getRegistry(REGISTRY_PORT);
            MapRedRMI mapper = (MapRedRMI) lookupRegistry.lookup(MASTER_LOOKUP);
            logger.info("Invoking the MapReduce Job!");
            String result = mapper.submitJob(WordMapper.class, WordReducer.class, INPUT_DIR);
            logger.info("Result: " + result);
        } catch (AccessException e) {
            logger.error("Error accessing the registry: " + e.getMessage(), e);
        } catch (RemoteException e) {
            logger.error("Error occurred while accessing the registry: "+ e.getMessage(), e);
        } catch (NotBoundException e) {
            logger.error("Error occurred while retrieving RPC bind: "+ e.getMessage(), e);
        }
    }
    public static class WordMapper implements Mapper {
        public void map(String input, Context context) {
            BufferedReader br;
            try {
                br = new BufferedReader(new FileReader(input));
                String line = null;
                while ((line = br.readLine()) != null) {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        context.write(word, "1");
                    }
                }
            } catch (FileNotFoundException e) {
                logger.error("The file " + input + " cannot be found: " + e.getMessage(), e);
            } catch (IOException e) {
                logger.error("Error accessing the file " + input + ": " + e.getMessage(), e);
            }
        }
    }

    public static class WordReducer implements Reducer {
        public String reduce(String key, Iterator<String> values) {
            int result = 0;
            while (values.hasNext()) {
                result += Integer.parseInt(values.next());
            }
            return String.valueOf(result);
        }
    }
}
