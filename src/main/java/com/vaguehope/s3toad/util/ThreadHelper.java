package com.vaguehope.s3toad.util;

public final class ThreadHelper {

	private ThreadHelper() {
		throw new AssertionError();
	}

	public static void sleepQuietly (long s) {
		try {
			Thread.sleep(s);
		}
		catch (InterruptedException e) { /* Do not care. */}
	}

}
