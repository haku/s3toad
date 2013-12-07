package com.vaguehope.s3toad.tasks;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.vaguehope.s3toad.C;
import com.vaguehope.s3toad.util.PrgTracker;
import com.vaguehope.s3toad.util.ThreadHelper;

public class UploadMulti {

	protected static final Logger LOG = LoggerFactory.getLogger(UploadMulti.class);

	public static final long DEFAULT_CHUNK_SIZE = 64L * 1024L * 1024L;
	private static final int PART_UPLOAD_RETRY_COUNT = 5;

	private final AmazonS3 s3Client;
	private final File file;
	private final String bucket;
	private final String key;
	private final ExecutorService executor;
	private final long chunkSize;
	private final Map<String, String> metadata;

	public UploadMulti(final AmazonS3 s3Client, final File file, final String bucket, final String key, final int threads, final long chunkSize, final Map<String, String> metadata) {
		this.s3Client = s3Client;
		this.file = file;
		this.bucket = bucket;
		this.key = key;
		this.executor = Executors.newFixedThreadPool(threads);
		this.chunkSize = chunkSize;
		this.metadata = metadata;
	}

	public UploadMulti(final AmazonS3 s3Client, final File file, final String bucket, final String key, final int threads, final long chunkSize) {
		this.s3Client = s3Client;
		this.file = file;
		this.bucket = bucket;
		this.key = key;
		this.executor = Executors.newFixedThreadPool(threads);
		this.chunkSize = chunkSize;
		this.metadata = new HashMap<String, String>();
	}

	public UploadMulti(final AmazonS3 s3Client, final File file, final String bucket, final String key, final ExecutorService executor, final long chunkSize, final Map<String, String> metadata) {
		this.s3Client = s3Client;
		this.file = file;
		this.bucket = bucket;
		this.key = key;
		this.executor = executor;
		this.chunkSize = chunkSize;
		this.metadata = metadata;
	}

	public UploadMulti(final AmazonS3 s3Client, final File file, final String bucket, final String key, final ExecutorService executor, final long chunkSize) {
		this.s3Client = s3Client;
		this.file = file;
		this.bucket = bucket;
		this.key = key;
		this.executor = executor;
		this.chunkSize = chunkSize;
		this.metadata = new HashMap<String, String>();
	}

	public void dispose() {
		this.executor.shutdown();
	}

	public File getFile () {
		return this.file;
	}

	public void run() throws Exception {
		if (!this.file.exists()) {
			LOG.warn("vanished={}", this.file.getAbsolutePath());
			return;
		}
		final long startTime = System.currentTimeMillis();
		final PrgTracker tracker = new PrgTracker(LOG);
		final long contentLength = this.file.length();
		final List<Future<UploadPartResult>> uploadFutures = new ArrayList<Future<UploadPartResult>>();
		// FIXME specify MD5.
		final ObjectMetadata objMetadata = new ObjectMetadata();
		objMetadata.setUserMetadata(this.metadata);
		final InitiateMultipartUploadResult initResponse = initiateMultipartUpload(new InitiateMultipartUploadRequest(this.bucket, this.key, objMetadata));
		try {
			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				long partSize = Math.min(this.chunkSize, (contentLength - filePosition));
				UploadPartRequest uploadRequest = new UploadPartRequest()
						.withBucketName(this.bucket).withKey(this.key)
						.withUploadId(initResponse.getUploadId()).withPartNumber(i)
						.withFileOffset(filePosition)
						.withFile(this.file)
						.withPartSize(partSize)
						.withProgressListener(tracker);
				uploadFutures.add(this.executor.submit(new PartUploader(this.s3Client, uploadRequest)));
				filePosition += partSize;
			}

			List<PartETag> partETags = new ArrayList<PartETag>();
			for (Future<UploadPartResult> future : uploadFutures) {
				partETags.add(future.get().getPartETag());
			}
			completeMultipartUpload(new CompleteMultipartUploadRequest(this.bucket, this.key, initResponse.getUploadId(), partETags));

			LOG.info("contentLength={} parts={} duration={}s",
					contentLength, uploadFutures.size(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime));
		}
		catch (Exception e) {
			this.s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(this.bucket, this.key, initResponse.getUploadId()));
			throw e;
		}
	}

	private InitiateMultipartUploadResult initiateMultipartUpload(final InitiateMultipartUploadRequest initRequest) throws Exception {
		int attempt = 0;
		while (true) {
			attempt++;
			try {
				return this.s3Client.initiateMultipartUpload(initRequest);
			}
			catch (Exception e) {
				if (attempt >= PART_UPLOAD_RETRY_COUNT) throw e;
				LOG.info("initiateMultipartUpload attempt {} failed: '{}'.  It will be retried.", attempt, e.getMessage());
				ThreadHelper.sleepQuietly(C.AWS_API_RETRY_DELAY_MILLES);
			}
		}
	}

	private void completeMultipartUpload(final CompleteMultipartUploadRequest compRequest) throws Exception {
		int attempt = 0;
		while (true) {
			attempt++;
			try {
				this.s3Client.completeMultipartUpload(compRequest);
				return;
			}
			catch (Exception e) {
				if (attempt >= PART_UPLOAD_RETRY_COUNT) throw e;
				LOG.info("completeMultipartUpload attempt {} failed: '{}'.  It will be retried.", attempt, e.getMessage());
				ThreadHelper.sleepQuietly(C.AWS_API_RETRY_DELAY_MILLES);
			}
		}
	}

	private static class PartUploader implements Callable<UploadPartResult> {

		private final AmazonS3 s3Client;
		private final UploadPartRequest uploadRequest;

		public PartUploader(final AmazonS3 s3Client, final UploadPartRequest uploadRequest) {
			this.s3Client = s3Client;
			this.uploadRequest = uploadRequest;
		}

		@Override
		public UploadPartResult call() throws Exception {
			int attempt = 0;
			while (true) {
				attempt++;
				try {
					return uploadPart();
				}
				catch (Exception e) {
					if (attempt >= PART_UPLOAD_RETRY_COUNT) throw e;
					LOG.info("Upload of part {} with length {} attempt {} failed: '{}'.  It will be retried.",
							this.uploadRequest.getPartNumber(), this.uploadRequest.getPartSize(), attempt, e.getMessage());
					ThreadHelper.sleepQuietly(C.AWS_API_RETRY_DELAY_MILLES);
				}
			}
		}

		private UploadPartResult uploadPart() {
			final long startTime = System.currentTimeMillis();
			UploadPartResult res = this.s3Client.uploadPart(this.uploadRequest);
			final long seconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
			LOG.info("part={} size={} duration={}s", this.uploadRequest.getPartNumber(), this.uploadRequest.getPartSize(), seconds);
			return res;
		}

	}

}
