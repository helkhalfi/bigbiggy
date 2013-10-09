package com.mcba.util.properties;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

public class PropertiesLoader {
	private static final Logger logger = LoggerFactory.getLogger(PropertiesLoader.class);
	private static final String CONFIG_FILE = "conf/mcba/mcba-cluster.properties";
	private static Properties properties;
	static {
		load();
	}
	private static void load() {
		URL url = Resources.getResource(CONFIG_FILE);
		InputSupplier<InputStream> inputSupplier = Resources.newInputStreamSupplier(url);
		properties = new Properties();
		try {
			properties.load(inputSupplier.getInput());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	
	public static String get(String key) {
		return properties.getProperty(key);
	}
	
	public static void main(String[] args) {
		
	}
}
