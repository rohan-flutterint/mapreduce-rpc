package iu.swithana.systems.mapreduce.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private Properties configurations;

    public Config() throws IOException {
        // if external property file is provided
        String configFilePath = System.getProperty( "config.file" );
        configurations = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream stream;
        if (configFilePath != null) {
            stream = new FileInputStream(configFilePath);
        } else {
            stream = loader.getResourceAsStream(Constants.CONFIG_FILE_NAME);
        }
        configurations.load(stream);
    }

    public String getConfig(String key) {
        return configurations.getProperty(key);
    }
}
