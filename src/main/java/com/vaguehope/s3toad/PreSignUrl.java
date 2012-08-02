package com.vaguehope.s3toad;

import java.net.URL;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

/**
 * http://docs.amazonwebservices.com/AmazonS3/latest/dev/ShareObjectPreSignedURLJavaSDK.html
 */
public class PreSignUrl {

	private final AmazonS3 s3Client;
	private final String bucket;
	private final String key;

	public PreSignUrl (AmazonS3 s3Client, String bucket, String key) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.key = key;
	}

	public void run () {
		java.util.Date expiration = new java.util.Date();
		long msec = expiration.getTime();
		msec += 1000 * 60 * 60; // 1 hour.
		expiration.setTime(msec);
		System.err.println("expiry=1h");

		GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(this.bucket, this.key);
		generatePresignedUrlRequest.setMethod(HttpMethod.GET); // Default.
		generatePresignedUrlRequest.setExpiration(expiration);
		URL url = this.s3Client.generatePresignedUrl(generatePresignedUrlRequest);
		System.err.println("url=" + url.toString());
	}

}
