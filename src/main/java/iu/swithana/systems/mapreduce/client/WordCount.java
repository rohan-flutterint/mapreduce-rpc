package iu.swithana.systems.mapreduce.client;

import iu.swithana.systems.mapreduce.config.Config;
import iu.swithana.systems.mapreduce.config.Constants;
import iu.swithana.systems.mapreduce.core.Context;
import iu.swithana.systems.mapreduce.core.Mapper;
import iu.swithana.systems.mapreduce.core.Reducer;
import iu.swithana.systems.mapreduce.master.MapRedRMI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;

public class WordCount {
    private static Logger logger = LoggerFactory.getLogger(WordCount.class);

    private static int REGISTRY_PORT;
    private static String REGISTRY_HOST;
    private static String MASTER_BIND;
    private static String INPUT_DIR;
//    private static final String INPUT_DIR = "/Users/swithana/git/mapreduce-rpc/input/WordCountTest.txt";

    public static void main(String[] args) {
        Registry lookupRegistry;
        try {
            // loading the configs
            Config config = new Config();
            REGISTRY_PORT = Integer.parseInt(config.getConfig(Constants.RMI_REGISTRY_PORT));
            REGISTRY_HOST = config.getConfig(Constants.RMI_REGISTRY_HOST);
            MASTER_BIND = config.getConfig(Constants.MASTER_BIND);
            INPUT_DIR = config.getConfig(Constants.INPUT_DIR);

            lookupRegistry = LocateRegistry.getRegistry(REGISTRY_PORT);
            MapRedRMI mapper = (MapRedRMI) lookupRegistry.lookup(MASTER_BIND);
            logger.info("Invoking the MapReduce Job!");
            String result = mapper.submitJob(WordMapper.class, WordReducer.class, INPUT_DIR);
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
    }

    public static class WordMapper implements Mapper {
        public void map(String input, Context context) {
            String[] lines = input.split("\n");
            for (String line : lines) {
                if (line != null || !line.equals("")) {
                    String[] words = line.replaceAll("[^a-zA-Z0-9]", " ").split(" ");
                    for (String word : words) {
                        context.write(word, "1");
                    }
                }
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
