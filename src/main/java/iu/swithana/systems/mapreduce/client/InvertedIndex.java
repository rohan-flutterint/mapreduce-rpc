package iu.swithana.systems.mapreduce.client;

import iu.swithana.systems.mapreduce.config.Config;
import iu.swithana.systems.mapreduce.config.Constants;
import iu.swithana.systems.mapreduce.common.JobContext;
import iu.swithana.systems.mapreduce.common.ResultMap;
import iu.swithana.systems.mapreduce.common.Mapper;
import iu.swithana.systems.mapreduce.common.Reducer;
import iu.swithana.systems.mapreduce.master.MapRedRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class InvertedIndex {
    private static Logger logger = LoggerFactory.getLogger(InvertedIndex.class);

    private static int REGISTRY_PORT;
    private static String REGISTRY_HOST;
    private static String MASTER_BIND;
    private static String INPUT_DIR;
    private static String OUTPUT_DIR;

    public static void main(String[] args) {
        runInvertedIndexProgram();
    }

    // taken this out of the main method to be able to test in the test cases
    protected static String runInvertedIndexProgram() {
        Registry lookupRegistry;
        String result = "";
        try {
            // loading the configs
            Config config = new Config();
            REGISTRY_PORT = Integer.parseInt(config.getConfig(Constants.RMI_REGISTRY_PORT));
            REGISTRY_HOST = config.getConfig(Constants.RMI_REGISTRY_HOST);
            MASTER_BIND = config.getConfig(Constants.MASTER_BIND);
            INPUT_DIR = config.getConfig(Constants.INPUT_DIR);
            OUTPUT_DIR = config.getConfig(Constants.OUTPUT_DIR);

            lookupRegistry = LocateRegistry.getRegistry(REGISTRY_PORT);
            MapRedRMI mapper = (MapRedRMI) lookupRegistry.lookup(MASTER_BIND);
            logger.info("Invoking the MapReduce Job!");
            result = mapper.submitJob(InvertedIndexMapper.class, InvertedIndexReducer.class, INPUT_DIR, OUTPUT_DIR);
            logger.info("Result: " + result);
        } catch (AccessException e) {
            logger.error("Error accessing the registry: " + e.getMessage(), e);
        } catch (RemoteException e) {
            logger.error("Error occurred while accessing the registry: " + e.getMessage(), e);
        } catch (NotBoundException e) {
            logger.error("Error occurred while retrieving RPC bind: " + e.getMessage(), e);
        } catch (IOException e) {
            logger.error("Error accessing the configuration file: "+ e.getMessage(), e);
        }
        return result;
    }

    public static class InvertedIndexMapper implements Mapper {
        public void map(String input, ResultMap resultMap, JobContext context) {
            String[] lines = input.split("\n");
            for (String line : lines) {
                if (line != null || !line.equals("")) {
                    String[] words = line.replaceAll("[^a-zA-Z0-9]", " ").split(" ");
                    for (String word : words) {
                        resultMap.write(word, (String) context.getConfig("filename"));
                    }
                }
            }
        }
    }

    public static class InvertedIndexReducer implements Reducer {
        public String reduce(String key, Iterator<String> values) {
            Set<String> results = new HashSet<>();
            while (values.hasNext()) {
                results.add(values.next());
            }
            return String.join(",", results);
        }
    }
}
