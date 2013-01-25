package com.vaguehope.s3toad;

import java.io.File;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class Main {

	private final AmazonS3 s3Client;

	public Main () throws MalformedURLException {
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		findProxy(clientConfiguration);
		AmazonS3Client s3c = new AmazonS3Client();
		s3c.setConfiguration(clientConfiguration);
		this.s3Client = s3c;
	}

	public void run (String[] rawArgs)  {
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
				case HELP:
				default:
					fullHelp(parser, err);
			}
		}
		catch (CmdLineException e) {
			err.println(e.getMessage());
			shortHelp(parser, err);
			return;
		}
		catch (AmazonClientException e) {
			err.println("An AWS exception occured: " + e.getMessage());
		}
		catch (Exception e) {
			err.println("An unhandled error occured.");
			e.printStackTrace(err);
		}
		err.println("done.");
	}

	private static void shortHelp (CmdLineParser parser, PrintStream ps) {
		ps.print("Usage: ");
		ps.print(C.APPNAME);
		parser.printSingleLineUsage(System.err);
		ps.println();
	}

	private static void fullHelp (CmdLineParser parser, PrintStream ps) {
		shortHelp(parser, ps);
		parser.printUsage(ps);
		ps.println();
	}

	private void doList (Args args) throws CmdLineException  {
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

	private void doPush (Args args) throws Exception  {
		final String filepath = args.getArg(0, true);
		final String bucket = args.getArg(1, true);
		String key = args.getArg(2, false);
		args.maxArgs(3);
		final int threads = args.getThreadCount(1);
		final long chunkSize = args.getChunkSize(UploadMulti.DEFAULT_CHUNK_SIZE);

		File file = new File(filepath);
		if (!file.exists()) {
			System.err.println("File not found: " + file.getAbsolutePath());
			return;
		}
		if (key == null) key = file.getName();

		System.err.println("file=" + file.getAbsolutePath());
		System.err.println("bucket=" + bucket);
		System.err.println("key=" + key);
		System.err.println("threads=" + threads);

		UploadMulti u = new UploadMulti(this.s3Client, file, bucket, key, threads, chunkSize);
		try {
			u.run();
		}
		finally {
			u.dispose();
		}
	}

	private void doPull (Args args) throws CmdLineException, AmazonClientException, InterruptedException  {
		final String bucket = args.getArg(0, true);
		final String key = args.getArg(1, true);
		args.maxArgs(2);

		System.err.println("bucket=" + bucket);
		System.err.println("key=" + key);

		new DownloadSimple(this.s3Client, bucket, key).run();
	}

	private void doUrl (Args args) throws CmdLineException {
		final String bucket = args.getArg(0, true);
		final String key = args.getArg(1, true);
		args.maxArgs(2);
		final int hours = args.getHours(1);

		System.err.println("bucket=" + bucket);
		System.err.println("key=" + key);
		System.err.println("hours=" + hours);

		new PreSignUrl(this.s3Client, bucket, key, hours).run();
	}

	private void doStatus (Args args) throws CmdLineException {
		String bucket = args.getArg(0, true);
		args.maxArgs(1);
		System.err.println("bucket=" + bucket);
		new Status(this.s3Client, bucket).run();
	}

	private void doClean (Args args) throws CmdLineException {
		String bucket = args.getArg(0, true);
		args.maxArgs(1);
		System.err.println("bucket=" + bucket);
		new Clean(this.s3Client, bucket).run();
	}

	private static void findProxy (ClientConfiguration clientConfiguration) throws MalformedURLException {
		String[] envVars = { "https_proxy", "http_proxy" };
		for (String var : envVars) {
			String proxy;
			if ((proxy = System.getenv(var)) != null) {
				setProxy(clientConfiguration, proxy);
				return;
			}
		}
	}

	private static void setProxy (ClientConfiguration clientConfiguration, String proxy) throws MalformedURLException {
		String p = proxy.startsWith("http") ? proxy : "http://" + proxy;
		URL u = new URL(p);
		clientConfiguration.setProxyHost(u.getHost());
		clientConfiguration.setProxyPort(u.getPort());
	}

//	- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	public static void main (String[] args) throws MalformedURLException  {
		new Main().run(args);
	}

}
