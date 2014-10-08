package com.vaguehope.s3toad.tasks;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class ListBucket {

	private final AmazonS3 s3Client;
	private final String bucket;
	private final String prefix;

	public ListBucket(final AmazonS3 s3Client, final String bucket, final String prefix) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.prefix = prefix;
	}

	public void run() {
		long totalSize = 0;
		long objectCount = 0;

		ObjectListing objectListing = this.s3Client.listObjects(this.bucket, this.prefix);
		// FIXME use nice ASCII table code from Lookfar.
		while (true) {
			for (S3ObjectSummary o : objectListing.getObjectSummaries()) {
				++objectCount;
				totalSize += o.getSize();
				System.out.println(String.format("%d\t%d\t%s\t%s", o.getLastModified().getTime(), o.getSize(), o.getKey(), o.getETag()));
			}
			if (objectListing.getNextMarker() == null) break;
			objectListing = this.s3Client.listObjects(new ListObjectsRequest()
					.withBucketName(this.bucket)
					.withPrefix(this.prefix)
					.withMarker(objectListing.getNextMarker()));
		}

		System.out.println("bucket=" + this.bucket + " objects=" + objectCount + " total_size=" + totalSize);
	}
}
