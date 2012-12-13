package com.vaguehope.s3toad;

import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;

public class ListBuckets {

	private final AmazonS3 s3Client;

	public ListBuckets(AmazonS3 s3Client) {
		this.s3Client = s3Client;
	}

	public void run() {
		List<Bucket> list = this.s3Client.listBuckets();
		for (Bucket b : list) {
			System.err.println(b.getName());
		}
	}

}
