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
	private final int hours;

	public PreSignUrl (AmazonS3 s3Client, String bucket, String key, int hours) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.key = key;
		this.hours = hours;
	}

	public void run () {
		java.util.Date expiration = new java.util.Date();
		long msec = expiration.getTime();
		msec += 1000 * 60 * 60 * this.hours;
		expiration.setTime(msec);
		System.err.println("expiry=" + this.hours + "h");

		GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(this.bucket, this.key);
		generatePresignedUrlRequest.setMethod(HttpMethod.GET); // Default.
		generatePresignedUrlRequest.setExpiration(expiration);
		URL url = this.s3Client.generatePresignedUrl(generatePresignedUrlRequest);
		System.err.println("url=" + url.toString());
	}

}
