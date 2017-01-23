package com.github.mdjc;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteTask implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);
	static final Future<String> POISON_FUTURE = new FutureTask<>(() -> "POISON_FUTURE");

	private final BlockingQueue<Future<String>> lines;
	private final Path path;

	public WriteTask(BlockingQueue<Future<String>> lines, Path path) {
		this.lines = lines;
		this.path = path;
	}

	@Override
	public void run() {
		BufferedWriter writer = null;
		try {
			writer = Files.newBufferedWriter(path);
			int lineCount = 0;
			Future<String> lineFuture;
			while ((lineFuture = lines.take()) != POISON_FUTURE) {
				String line = lineFuture.get();
				writer.write(line);
				writer.newLine();
				lineCount++;
				if (lineCount % 500 == 0) {
					logger.info("processed {} lines", lineCount);
				}
			}
			writer.flush();
			logger.info("processed {} lines", lineCount);
		} catch (Error | RuntimeException e) {
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		} finally {
			try {
				writer.close();
			} catch (Throwable ignored) {
			}
		}
	}
}
