package com.vaguehope.s3toad.tasks;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;

public class ListBuckets {

	private final AmazonS3 s3Client;

	public ListBuckets(AmazonS3 s3Client) {
		this.s3Client = s3Client;
	}

	public void run() {
		for (Bucket b : this.s3Client.listBuckets()) {
			System.out.print("bucket=");
			System.out.println(b.getName());
		}
	}

}
