package com.vaguehope.s3toad;

import java.io.File;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.vaguehope.s3toad.tasks.Clean;
import com.vaguehope.s3toad.tasks.DownloadSimple;
import com.vaguehope.s3toad.tasks.EmptyBucket;
import com.vaguehope.s3toad.tasks.LargeCopy;
import com.vaguehope.s3toad.tasks.ListBucket;
import com.vaguehope.s3toad.tasks.ListBuckets;
import com.vaguehope.s3toad.tasks.PreSignUrl;
import com.vaguehope.s3toad.tasks.Status;
import com.vaguehope.s3toad.tasks.UploadMulti;
import com.vaguehope.s3toad.tasks.WatchUpload;
import com.vaguehope.s3toad.util.LogHelper;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Main {

	private static final String S3_ENDPOINT = "s3-eu-west-1.amazonaws.com";

	private final AmazonS3 s3Client;

	static class VoodooAmazonS3Client extends AmazonS3Client {

		@Override
		public <X extends AmazonWebServiceRequest> Request<X> createRequest (final String bucketName, final String key, final X originalRequest, final HttpMethodName httpMethod) {
			final Request<X> res = super.createRequest(bucketName, key, originalRequest, httpMethod);
			System.err.println("requestEndpoint=" + res.getEndpoint());
			return res;
		}

	}

	public Main() throws MalformedURLException {
		LogHelper.bridgeJul();
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		findProxy(clientConfiguration);
		AmazonS3Client s3c = new VoodooAmazonS3Client();
		s3c.setConfiguration(clientConfiguration);
		s3c.setEndpoint(S3_ENDPOINT);
		System.err.println("defaultEndpoint=" + S3_ENDPOINT);
		this.s3Client = s3c;
	}

	public void run (final String[] rawArgs) {
		//final PrintStream out = System.out;
		final PrintStream err = System.err;
		final Args args = new Args();
		final CmdLineParser parser = new CmdLineParser(args);
		try {
			parser.parseArgument(rawArgs);
			switch (args.getAction()) {
				case LIST:
					doList(args);
					break;
				case PUSH:
					doPush(args);
					break;
				case WATCH:
					doWatch(args);
					break;
				case PULL:
					doPull(args);
					break;
				case URL:
					doUrl(args);
					break;
				case STATUS:
					doStatus(args);
					break;
				case CLEAN:
					doClean(args);
					break;
				case EMPTY:
					doEmpty(args);
					break;
				case COPY:
					doLargeCopy(args);
					break;
                case METADATA:
                    doMetadata(args);
                    break;
				case HELP:
				default:
					fullHelp(parser, err);
			}
		}
		catch (CmdLineException e) {
			err.println(e.getMessage());
			shortHelp(parser, err);
			System.exit(1);
			return;
		}
		catch (AmazonClientException e) {
			err.println("An AWS exception occured: " + e.toString());
			e.printStackTrace(err);
			System.exit(2);
		}
		catch (Exception e) {
			err.println("An unhandled error occured.");
			e.printStackTrace(err);
			System.exit(3);
		}
		err.println("success.");
	}

	private static void shortHelp (final CmdLineParser parser, final PrintStream ps) {
		ps.print("Usage: ");
		ps.print(C.APPNAME);
		parser.printSingleLineUsage(System.err);
		ps.println();
	}

	private static void fullHelp (final CmdLineParser parser, final PrintStream ps) {
		shortHelp(parser, ps);
		parser.printUsage(ps);
		ps.println();
	}

	private void doLargeCopy (final Args args) throws CmdLineException, InterruptedException, ExecutionException {
		final String sourceBucket = args.getArg(0, true);
		final String sourceKey = args.getArg(1, true);
		final String destinationBucket = args.getArg(2, true);
		final String destinationKey = args.getArg(3, true);
        final Map<String, String> metadata = args.getMetadata();
		args.minArgs(4);

        new LargeCopy(s3Client, sourceBucket, sourceKey, destinationBucket, destinationKey, metadata).call();
	}

	private void doList (final Args args) throws CmdLineException {
		String bucket = args.getArg(0, false);
		args.maxArgs(1);
		if (bucket != null) {
			System.err.println("bucket=" + bucket);
			new ListBucket(this.s3Client, bucket).run();
		}
		else {
			new ListBuckets(this.s3Client).run();
		}
	}

	private void doPush (final Args args) throws Exception {
		final String filepath = args.getArg(0, true);
		final String bucket = args.getArg(1, true);
		String key = args.getArg(2, false);
		args.minArgs(3);
        Map<String, String> metadata = args.getMetadata();
		final int threads = args.getThreadCount(1);
		final long chunkSize = args.getChunkSize(UploadMulti.DEFAULT_CHUNK_SIZE);

		final File file = new File(filepath);
		if (!file.exists()) {
			System.err.println("File not found: " + file.getAbsolutePath());
			return;
		}
		if (key == null) key = file.getName();

		System.err.println("file=" + file.getAbsolutePath());
		System.err.println("bucket=" + bucket);
		System.err.println("key=" + key);
		System.err.println("threads=" + threads);
		System.err.println("chunkSize=" + chunkSize);

		UploadMulti u = new UploadMulti(this.s3Client, file, bucket, key, threads, chunkSize, metadata);
		try {
			u.run();
		}
		finally {
			u.dispose();
		}
	}

	private void doWatch (final Args args) throws Exception {
		final String dirpath = args.getArg(0, true);
		final String bucket = args.getArg(1, true);
		args.maxArgs(2);
		final int workerThreads = args.getThreadCount(1);
		final int controlTrheads = args.getControlThreads(1);
		final long chunkSize = args.getChunkSize(UploadMulti.DEFAULT_CHUNK_SIZE);
		final boolean deleteAfter = args.getDelete();

		final File dir = new File(dirpath).getCanonicalFile();
		if (!dir.exists() || !dir.isDirectory()) {
			System.err.println("Dir not found: " + dir.getAbsolutePath());
			return;
		}

		System.err.println("dir=" + dir.getAbsolutePath());
		System.err.println("bucket=" + bucket);
		System.err.println("workerThreads=" + workerThreads);
		System.err.println("controlThreads=" + controlTrheads);
		System.err.println("chunkSize=" + chunkSize);
		System.err.println("deleteAfter=" + deleteAfter);

		WatchUpload u = new WatchUpload(this.s3Client, dir, bucket, workerThreads, controlTrheads, chunkSize, deleteAfter);
		try {
			u.run();
		}
		finally {
			u.dispose();
		}
	}

	private void doPull (final Args args) throws CmdLineException, AmazonClientException, InterruptedException {
		final String bucket = args.getArg(0, true);
		final String key = args.getArg(1, true);
		args.maxArgs(2);

		System.err.println("bucket=" + bucket);
		System.err.println("key=" + key);

		new DownloadSimple(this.s3Client, bucket, key).run();
	}

	private void doUrl (final Args args) throws CmdLineException {
		final String bucket = args.getArg(0, true);
		final String key = args.getArg(1, true);
		args.maxArgs(2);
		final int hours = args.getHours(1);

		System.err.println("bucket=" + bucket);
		System.err.println("key=" + key);
		System.err.println("hours=" + hours);

		new PreSignUrl(this.s3Client, bucket, key, hours).run();
	}

    private void doMetadata(final Args args) throws CmdLineException {
		String bucket = args.getArg(0, true);
		String key = args.getArg(1, true);
		args.maxArgs(2);

        ObjectMetadata metadata = s3Client.getObjectMetadata(bucket, key);
        for (Map.Entry<String, String> entry : metadata.getUserMetadata().entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }

    }

	private void doStatus (final Args args) throws CmdLineException {
		String bucket = args.getArg(0, true);
		args.maxArgs(1);
		System.err.println("bucket=" + bucket);
		new Status(this.s3Client, bucket).run();
	}

	private void doClean (final Args args) throws CmdLineException {
		String bucket = args.getArg(0, true);
		args.maxArgs(1);
		System.err.println("bucket=" + bucket);
		new Clean(this.s3Client, bucket).run();
	}

	private void doEmpty (final Args args) throws CmdLineException {
		String bucket = args.getArg(0, true);
		args.maxArgs(1);
		System.err.println("bucket=" + bucket);
		new EmptyBucket(this.s3Client, bucket).run();
	}

	private static void findProxy (final ClientConfiguration clientConfiguration) throws MalformedURLException {
		String[] envVars = { "https_proxy", "http_proxy" };
		for (String var : envVars) {
			String proxy;
			if ((proxy = System.getenv(var)) != null) {
				setProxy(clientConfiguration, proxy);
				return;
			}
		}
	}

	private static void setProxy (final ClientConfiguration clientConfiguration, final String proxy) throws MalformedURLException {
		String p = proxy.startsWith("http") ? proxy : "http://" + proxy;
		URL u = new URL(p);
		clientConfiguration.setProxyHost(u.getHost());
		clientConfiguration.setProxyPort(u.getPort());
	}

//	- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	public static void main (final String[] args) throws MalformedURLException {
		new Main().run(args);
	}

}
