package com.vaguehope.s3toad;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public class UploadMulti {

	private static long PART_SIZE = 64L * 1024L * 1024L;

	private final AmazonS3 s3Client;
	private final File file;
	private final String bucket;
	private String key;
	private final ExecutorService executor;


	public UploadMulti (AmazonS3 s3Client, File file, String bucket, String key, int threads) {
		this.s3Client = s3Client;
		this.file = file;
		this.bucket = bucket;
		this.key = key;
		this.executor = Executors.newFixedThreadPool(threads);
	}

	public void dispose () {
		this.executor.shutdown();
	}

	public void run () throws Exception {
		long contentLength = this.file.length();

		List<Future<UploadPartResult>> uploadFutures = new ArrayList<Future<UploadPartResult>>();
		PrgTracker tracker = new PrgTracker();

		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(this.bucket, this.key);
		InitiateMultipartUploadResult initResponse = this.s3Client.initiateMultipartUpload(initRequest);

		final long startTime = System.currentTimeMillis();
		try {
			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				PART_SIZE = Math.min(PART_SIZE, (contentLength - filePosition));
				UploadPartRequest uploadRequest = new UploadPartRequest()
						.withBucketName(this.bucket).withKey(this.key)
						.withUploadId(initResponse.getUploadId()).withPartNumber(i)
						.withFileOffset(filePosition)
						.withFile(this.file)
						.withPartSize(PART_SIZE)
						.withProgressListener(tracker);
				uploadFutures.add(this.executor.submit(new PartUploader(this.s3Client, uploadRequest)));
				filePosition += PART_SIZE;
			}
			List<PartETag> partETags = new ArrayList<PartETag>();
			for (Future<UploadPartResult> future : uploadFutures) {
				partETags.add(future.get().getPartETag());
			}
			CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(this.bucket, this.key, initResponse.getUploadId(), partETags);
			this.s3Client.completeMultipartUpload(compRequest);

			tracker.print();
			System.err.println("duration=" + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) + "s");
		}
		catch (Exception e) {
			this.s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(this.bucket, this.key, initResponse.getUploadId()));
			throw e;
		}
	}

	private static class PartUploader implements Callable<UploadPartResult> {

		private final AmazonS3 s3Client;
		private final UploadPartRequest uploadRequest;

		public PartUploader (AmazonS3 s3Client, UploadPartRequest uploadRequest) {
			this.s3Client = s3Client;
			this.uploadRequest = uploadRequest;
		}

		@Override
		public UploadPartResult call () throws Exception {
			return this.s3Client.uploadPart(this.uploadRequest);
		}

	}

	private static class PrgTracker implements ProgressListener {

		private final AtomicLong total = new AtomicLong(0);
		private final AtomicLong lastUpdate = new AtomicLong(0);

		public PrgTracker () {}

		@Override
		public void progressChanged (ProgressEvent progressEvent) {
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

		private boolean shouldPrint () {
			return System.currentTimeMillis() - this.lastUpdate.get() > 2000L;
		}

		public void print () {
			System.err.println("transfered=" + this.total.get());
		}

	}

}
