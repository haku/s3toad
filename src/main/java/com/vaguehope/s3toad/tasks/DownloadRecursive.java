package com.vaguehope.s3toad.tasks;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

public class DownloadRecursive {

	private static final Logger LOG = LoggerFactory.getLogger(DownloadRecursive.class);

	private final AmazonS3 s3Client;
	private final String bucket;
	private final String prefix;

	public DownloadRecursive(final AmazonS3 s3Client, final String bucket, final String prefix) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.prefix = prefix;
	}

	public void run() throws InterruptedException, IOException {
		LOG.info("counting...");
		final List<S3ObjectSummary> objects = new ArrayList<S3ObjectSummary>();
		ObjectListing objectListing = this.s3Client.listObjects(new ListObjectsRequest()
				.withBucketName(this.bucket)
				.withPrefix(this.prefix));
		while (true) {
			objects.addAll(objectListing.getObjectSummaries());
			if (objectListing.getNextMarker() == null) break;
			objectListing = this.s3Client.listObjects(new ListObjectsRequest()
					.withBucketName(this.bucket)
					.withPrefix(this.prefix)
					.withMarker(objectListing.getNextMarker()));
		}
		LOG.info("itemCount={}", objects.size());

		final File baseDir = new File(basename(this.prefix)).getAbsoluteFile();
		LOG.info("baseDir={}", baseDir.getAbsolutePath());

		final TransferManager tm = new TransferManager(this.s3Client);
		try {
			for (final S3ObjectSummary o : objects) {
				if (!o.getKey().startsWith(this.prefix)) throw new IllegalStateException("S3 listing returned key that did not start with requested prefix: " + o.getKey());
				final String localPath = o.getKey().substring(this.prefix.length());
				final File localFile = new File(baseDir, localPath);
				if (!localFile.exists() || localFile.lastModified() != o.getLastModified().getTime()) {
					LOG.info("{} --> {}", o.getKey(), localFile.getAbsolutePath());
					mkdirParentDirs(localFile);
					tm.download(new GetObjectRequest(o.getBucketName(), o.getKey()), localFile).waitForCompletion();
					localFile.setLastModified(o.getLastModified().getTime());
				}
			}
		}
		finally {
			tm.shutdownNow();
		}
	}

	private static String basename(String n) {
		if (n == null) return null;
		n = n.replaceAll("/*$", "");
		if (!n.contains("/")) return n;
		return n.substring(n.lastIndexOf("/") + 1);
	}

	private static void mkdirParentDirs(final File file) throws IOException {
		final File dir = file.getParentFile();
		if (dir == null) throw new IOException("File has no parent: " + file);
		if (dir.isDirectory()) return;
		if (!dir.mkdirs()) throw new IOException("Failed mkdirs: " + dir.getAbsolutePath());
	}

}
