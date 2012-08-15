package com.vaguehope.s3toad;

import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;

public class Status {

	private final AmazonS3 s3Client;
	private final String bucket;

	public Status (AmazonS3 s3Client, String bucket) {
		this.s3Client = s3Client;
		this.bucket = bucket;
	}

	public void run () {
		MultipartUploadListing listMultipartUploads = this.s3Client.listMultipartUploads(new ListMultipartUploadsRequest(this.bucket));
		System.err.println("uploads=" + listMultipartUploads.getMultipartUploads().size());
		for (MultipartUpload u : listMultipartUploads.getMultipartUploads()) {
			long ageDays = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis() - u.getInitiated().getTime());
			System.err.print("upload: key=");
			System.err.print(u.getKey());
			System.err.print(" id=");
			System.err.print(u.getUploadId());
			System.err.print(" age=");
			System.err.print(ageDays);
			System.err.print("d");
			System.err.println();
		}
	}

}
