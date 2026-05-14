package com.strandls.esmodule;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.core.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various configurations needed by the web app
 *
 * @author mukund
 */
public class ESmoduleConfig {

	private static Configuration config;

	private static final Logger logger = LoggerFactory.getLogger(ESmoduleConfig.class);

	private ESmoduleConfig() {
	}

	static {
		Configurations configs = new Configurations();
		try {
			config = configs.properties(new File("config.properties"));
		} catch (ConfigurationException cex) {
			logger.error("Error while reading configuration. Message {}", cex.getMessage());
		}
	}

	public static String getString(String key) {
		return config.getString(key);
	}

	public static int getInt(String key) {
		return config.getInt(key);
	}
	
	public static String fetchFileAsString(String fileName) throws IOException {
        InputStream in = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(fileName);
        if (in == null) {
            throw new IOException("File not found in classpath: " + fileName);
        }
        return IOUtils.toString(new InputStreamReader(in));
    }
}
