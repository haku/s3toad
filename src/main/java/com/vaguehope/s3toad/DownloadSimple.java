package com.vaguehope.s3toad;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;

public class DownloadSimple {

	private final AmazonS3 s3Client;
	private final String bucket;
	private final String key;

	public DownloadSimple (AmazonS3 s3Client, String bucket, String key) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.key = key;
	}

	public void run () throws AmazonClientException, InterruptedException {
		File localFile = new File(new File(this.key).getName());
		System.err.println("localFile=" + localFile.getAbsolutePath());
		TransferManager tm = new TransferManager(this.s3Client);
		try {
			PrgTracker tracker = new PrgTracker();
			final long startTime = System.currentTimeMillis();
			Download download = tm.download(
					new GetObjectRequest(this.bucket, this.key)
							.withProgressListener(tracker),
					localFile);
			System.err.println("contentLength=" + download.getObjectMetadata().getContentLength());
			download.waitForCompletion();
			tracker.print();
			System.err.println("duration=" + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) + "s");
		}
		finally {
			tm.shutdownNow();
		}
	}

}
