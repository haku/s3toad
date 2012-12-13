package com.vaguehope.s3toad;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class ListBucket {

	private final AmazonS3 s3Client;
	private final String bucket;

	public ListBucket(AmazonS3 s3Client, String bucket) {
		this.s3Client = s3Client;
		this.bucket = bucket;
	}

	public void run() {
		ObjectListing list = this.s3Client.listObjects(
				new ListObjectsRequest()
						.withBucketName(this.bucket)
				);
		for (S3ObjectSummary o : list.getObjectSummaries()) {
			System.err.println(o.getKey());
		}
	}

}
