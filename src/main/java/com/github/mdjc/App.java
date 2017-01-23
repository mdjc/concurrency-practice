package com.github.mdjc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
		Properties properties = loadConfig();
		Path inputPath = Paths.get(properties.getProperty("input.file.path"));
		Path outputPath = Paths.get(properties.getProperty("output.file.path"));
		int queueCapacity = Integer.valueOf(properties.getProperty("queue.capacity"));
		FileProcessor.run(inputPath, outputPath, queueCapacity);
		logger.info("Completed!");
	}

	private static Properties loadConfig() throws FileNotFoundException, IOException {
		try (InputStream is = new FileInputStream("app.properties");) {
			Properties properties = new Properties();
			properties.load(is);
			return properties;
		}
	}
}
