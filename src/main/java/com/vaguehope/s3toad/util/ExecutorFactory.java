package com.vaguehope.s3toad.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ExecutorFactory {

	private ExecutorFactory() {
		throw new AssertionError();
	}

	public static ThreadPoolExecutor newFixedThreadPool(String name, int nThreads) {
		return new ThreadPoolExecutor(nThreads, nThreads,
				0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(),
				new NamedThreadFactory(name));
	}

}
