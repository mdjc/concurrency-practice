package com.github.mdjc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileProcessor {
	private static final Logger logger = LoggerFactory.getLogger(FileProcessor.class);
	private static final int THREADS = Runtime.getRuntime().availableProcessors();

	public static void run(Path inputPath, Path outputPath, int queueCapacity) throws IOException, InterruptedException {
		ThreadPoolExecutor executor = null;
		BufferedReader reader = null;
		try {
			executor = new ThreadPoolExecutor(THREADS * 45, THREADS * 45, 0, TimeUnit.SECONDS,
					new LinkedBlockingQueue<>(queueCapacity));
			long startTime = System.currentTimeMillis();
			BlockingQueue<Future<String>> processedLines = new LinkedBlockingQueue<>(queueCapacity);
			executor.submit(new WriteTask(processedLines, outputPath));
			
			reader = Files.newBufferedReader(inputPath);
			String line;
			while ((line = reader.readLine()) != null) {
				Future<String> processedLine = executor.submit(new ProcessLineTask(line));
				processedLines.put(processedLine);
			}
			processedLines.put(WriteTask.POISON_FUTURE);
			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
			long elapsedTime = System.currentTimeMillis() - startTime;
			logger.info("Completed all activities with {} threads, in {} millis", executor.getMaximumPoolSize(), elapsedTime);
		} finally {
			try {
				reader.close();
			} catch (Throwable ignored) {
			}
			try {
				executor.shutdownNow();
			} catch (Throwable ignored) {
			}
		}
	}

}
