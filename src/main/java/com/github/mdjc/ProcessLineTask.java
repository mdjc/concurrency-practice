package com.github.mdjc;

import java.util.concurrent.Callable;

public class ProcessLineTask implements Callable<String> {
	private final String line;

	public ProcessLineTask(String line) {
		this.line = line;
	}

	@Override
	public String call() throws Exception {
		Thread.sleep(200);
		return line.toUpperCase();
	}
}
