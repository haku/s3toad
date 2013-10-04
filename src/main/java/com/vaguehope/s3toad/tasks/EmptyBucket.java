package com.vaguehope.s3toad.tasks;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class EmptyBucket {

	private final AmazonS3 s3Client;
	private final String bucket;

	public EmptyBucket(final AmazonS3 s3Client, final String bucket) {
		this.s3Client = s3Client;
		this.bucket = bucket;
	}

	public void run() {
		long totalSize = 0;
		long objectCount = 0;

		ObjectListing objectListing = this.s3Client.listObjects(this.bucket);
		while (true) {
			for (final S3ObjectSummary o : objectListing.getObjectSummaries()) {
				++objectCount;
				totalSize += o.getSize();
				this.s3Client.deleteObject(this.bucket, o.getKey());
				System.out.println(String.format("deleted key=%s size=%d", o.getKey(), o.getSize()));
			}
			if (objectListing.getNextMarker() == null) break;
			objectListing = this.s3Client.listObjects(new ListObjectsRequest()
					.withBucketName(this.bucket)
					.withMarker(objectListing.getNextMarker()));
		}

		System.out.println("emptied bucket=" + this.bucket + " objects=" + objectCount + " total_size=" + totalSize);
	}

}
