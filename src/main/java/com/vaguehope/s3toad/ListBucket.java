package com.vaguehope.s3toad;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class ListBucket {

	private final AmazonS3 s3Client;
	private final String bucket;

	public ListBucket(AmazonS3 s3Client, String bucket) {
		this.s3Client = s3Client;
		this.bucket = bucket;
	}

	public void run() {
		long totalSize = 0;
		long objectCount = 0;

		for (S3ObjectSummary o : this.s3Client.listObjects(this.bucket).getObjectSummaries()) {
			++objectCount;
			totalSize += o.getSize();
			System.out.println(String.format("%d\t%d\t%s", o.getLastModified().getTime(), o.getSize(), o.getKey()));
		}

		System.out.println("bucket=" + bucket + " objects=" + objectCount + " total_size=" + totalSize);
	}

}
