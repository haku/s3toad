package com.vaguehope.s3toad.tasks;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LargeCopy implements Callable<Void> {
	private final AmazonS3 s3Client;

    private final String sourceBucket;
	private final String sourceKey;
	private final String destinationBucket;
	private final String destinationKey;

    public LargeCopy(AmazonS3 s3Client, String sourceBucket, String sourceKey, String destinationBucket, String destinationKey) {
        this.s3Client = s3Client;
        this.sourceBucket = sourceBucket;
        this.sourceKey = sourceKey;
        this.destinationBucket = destinationBucket;
        this.destinationKey = destinationKey;
    }

    @Override
    public Void call() throws InterruptedException, ExecutionException {
		ObjectMetadata metadata = s3Client.getObjectMetadata(sourceBucket, sourceKey);

		InitiateMultipartUploadRequest startRequest = new InitiateMultipartUploadRequest(destinationBucket, destinationKey);
		final InitiateMultipartUploadResult startResult = s3Client.initiateMultipartUpload(startRequest);

		long start = 0;
		long max = 100L*1024L*1024L;
		System.out.println("Content Length: " + metadata.getContentLength() + " [max part length: " + max + "] -> estimated parts: " + (1+(metadata.getContentLength()/max)));

		System.out.println("start: " + new Date().toString());

		ExecutorService ex = Executors.newFixedThreadPool(50);
		List<Future<CopyPartResult>> futures = new ArrayList<Future<CopyPartResult>>();

		int partNumber = 1;
		while (start < metadata.getContentLength()) {
			long change = Math.min(max, metadata.getContentLength() - start);
			long end = start + change-1;

			final long actualStart = start;
			final long actualEnd = end;
			final int actualPartNumber = partNumber;

			System.out.println("Setting up part " + partNumber + " [" + start + ", " + end + "]");
			Callable<CopyPartResult> callable = new Callable<CopyPartResult>() {
				@Override public CopyPartResult call() throws Exception {
					CopyPartRequest partRequest = new CopyPartRequest()
							.withUploadId(startResult.getUploadId())
							.withFirstByte(actualStart)
							.withLastByte(actualEnd)
							.withSourceBucketName(sourceBucket)
							.withSourceKey(sourceKey)
							.withDestinationBucketName(destinationBucket)
							.withDestinationKey(destinationKey)
							.withPartNumber(actualPartNumber)
							;
				   return s3Client.copyPart(partRequest);
				}
			};
			Future<CopyPartResult> future = ex.submit(callable);
			futures.add(future);

			start += change;
			partNumber++;
		}

		List<PartETag> etags = new ArrayList<PartETag>();
		for (Future<CopyPartResult> future : futures) {
			CopyPartResult partResult = future.get();
			System.out.println("Processing part " + partResult.getPartNumber());
			etags.add(new PartETag(partResult.getPartNumber(), partResult.getETag()));
		}

		CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest((String)null, (String)null, (String)null, (List<PartETag>)null)
				.withUploadId(startResult.getUploadId())
				.withBucketName(destinationBucket)
				.withKey(destinationKey)
				.withPartETags(etags)
				;

		CompleteMultipartUploadResult completeResult = s3Client.completeMultipartUpload(completeRequest);
		System.out.println("end: " + new Date().toString());

		ex.shutdown();

        return null;
    }

}
