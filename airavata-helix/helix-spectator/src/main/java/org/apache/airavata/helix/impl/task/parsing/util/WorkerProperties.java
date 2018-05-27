package org.apache.airavata.helix.impl.task.parsing.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class WorkerProperties {

    private final static Logger logger = LoggerFactory.getLogger(WorkerProperties.class);

    private final static String WORKER_PROPERTY_FILE = "/conf/datacat-worker.properties";
    private static WorkerProperties instance;
    private Properties properties = null;

    private WorkerProperties() throws IOException {
        InputStream fileInput;
        if (new File(WORKER_PROPERTY_FILE).exists()) {
            fileInput = new FileInputStream(WORKER_PROPERTY_FILE);
            logger.info("Using configured worker property (datacat-worker.properties) file");

        } else {
            logger.error("Error finding the datacat-worker.properties file");
            throw new FileNotFoundException("Error finding the datacat-worker.properties file");
        }
        Properties properties = new Properties();
        properties.load(fileInput);
        fileInput.close();
        this.properties = properties;
    }

    public static WorkerProperties getInstance() throws IOException {
        if (instance == null) {
            instance = new WorkerProperties();
        }
        return instance;
    }

    public String getProperty(String key, String defaultVal) {
        String val = this.properties.getProperty(key);
        if (val.isEmpty()) {
            return defaultVal;
        } else {
            return val;
        }
    }
}
