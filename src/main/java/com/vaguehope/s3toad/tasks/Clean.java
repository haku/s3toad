package com.vaguehope.s3toad.tasks;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;

public class Clean {

	private final AmazonS3 s3Client;
	private final String bucket;

	public Clean (AmazonS3 s3Client, String bucket) {
		this.s3Client = s3Client;
		this.bucket = bucket;
	}

	public void run () {
		MultipartUploadListing listMultipartUploads = this.s3Client.listMultipartUploads(new ListMultipartUploadsRequest(this.bucket));
		System.err.println("uploads=" + listMultipartUploads.getMultipartUploads().size());
		for (MultipartUpload u : listMultipartUploads.getMultipartUploads()) {
			this.s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(this.bucket, u.getKey(), u.getUploadId()));
			System.err.print("cleaned: key=");
			System.err.print(u.getKey());
			System.err.print(" id=");
			System.err.print(u.getUploadId());
			System.err.println();
		}
	}

}
