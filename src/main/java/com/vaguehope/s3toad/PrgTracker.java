package com.vaguehope.s3toad;

import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;

class PrgTracker implements ProgressListener {

	private final AtomicLong total = new AtomicLong(0);
	private final AtomicLong lastUpdate = new AtomicLong(0);

	public PrgTracker() {}

	@Override
	public void progressChanged(ProgressEvent progressEvent) {
		this.total.addAndGet(progressEvent.getBytesTransfered());
		if (shouldPrint()) {
			synchronized (this.lastUpdate) {
				if (shouldPrint()) {
					print();
					this.lastUpdate.set(System.currentTimeMillis());
				}
			}
		}
	}

	private boolean shouldPrint() {
		return System.currentTimeMillis() - this.lastUpdate.get() > 2000L;
	}

	public void print() {
		System.err.println("transfered=" + this.total.get());
	}

}