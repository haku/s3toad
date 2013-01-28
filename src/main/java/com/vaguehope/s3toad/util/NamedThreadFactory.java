package com.vaguehope.s3toad.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

	private final AtomicInteger counter = new AtomicInteger(0);
	private final String namePrefix;

	public NamedThreadFactory (String namePrefix) {
		this.namePrefix = namePrefix;
	}

	@Override
	public Thread newThread (Runnable r) {
		Thread t = new Thread(
				Thread.currentThread().getThreadGroup(),
				r,
				this.namePrefix + "-" + this.counter.getAndIncrement());
		t.setDaemon(true);
		t.setPriority(Thread.NORM_PRIORITY);
		return t;
	}

}
