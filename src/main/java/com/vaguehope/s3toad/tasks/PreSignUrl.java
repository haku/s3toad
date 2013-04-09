package com.vaguehope.s3toad.tasks;

import java.net.URL;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

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

	public PreSignUrl (final AmazonS3 s3Client, final String bucket, final String key, final int hours) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.key = key;
		this.hours = hours;
	}

	public void run () {
		final long exp = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(this.hours);
		final Date expDate = new Date(exp);
		System.err.println("expiry=" + DateFormat.getDateTimeInstance().format(expDate));
		GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(this.bucket, this.key);
		generatePresignedUrlRequest.setMethod(HttpMethod.GET); // Default.
		generatePresignedUrlRequest.setExpiration(expDate);
		URL url = this.s3Client.generatePresignedUrl(generatePresignedUrlRequest);
		System.err.println("url=" + url.toString());
	}

}
