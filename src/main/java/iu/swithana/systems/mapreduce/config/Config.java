package iu.swithana.systems.mapreduce.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private Properties configurations;

    public Config() throws IOException {
        configurations = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream stream = loader.getResourceAsStream("config.properties");
        configurations.load(stream);
    }

    public String getConfig(String key) {
        return configurations.getProperty(key);
    }
}
