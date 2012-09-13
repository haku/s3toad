package com.vaguehope.s3toad;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

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

	public void run (String[] args) throws Exception {
		if (args.length < 1) {
			usage();
			return;
		}

		String act = args[0];
		String[] remainingArgs = Arrays.copyOfRange(args, 1, args.length);
		if ("push".equalsIgnoreCase(act)) {
			doPush(remainingArgs);
		}
		else if ("url".equalsIgnoreCase(act)) {
			doUrl(remainingArgs);
		}
		else if ("status".equalsIgnoreCase(act)) {
			doStatus(remainingArgs);
		}
		else if ("clean".equalsIgnoreCase(act)) {
			doClean(remainingArgs);
		}
		else {
			System.err.println("Unknown action: " + act);
			usage();
		}
		System.err.println("done.");
	}

	private static void usage () {
		System.err.println("Usage:\n" +
				"  push [local file path] [bucket] [threads]\n" +
				"  status [bucket]\n" +
				"  clean [bucket]\n" +
				"  url [bucket] [key] (hours)"
				);
	}

	private void doPush (String[] args) throws Exception {
		if (args.length < 3) {
			usage();
			return;
		}

		String filepath = args[0];
		String bucket = args[1];
		String threadsRaw = args[2];

		File file = new File(filepath);
		if (!file.exists()) {
			System.err.println("File not found: " + file.getAbsolutePath());
			return;
		}
		System.err.println("file=" + file.getAbsolutePath());
		System.err.println("bucket=" + bucket);

		String key = file.getName();
		System.err.println("key=" + key);

		int threads = Integer.parseInt(threadsRaw);
		System.err.println("threads=" + threads);

		UploadMulti u = new UploadMulti(this.s3Client, file, bucket, key, threads);
		try {
			u.run();
		}
		finally {
			u.dispose();
		}
	}

	private void doUrl (String[] args) {
		if (args.length < 2) {
			usage();
			return;
		}

		String bucket = args[0];
		String key = args[1];
		int hours = Integer.parseInt(args.length >= 3 ? args[2] : "1");
		System.err.println("bucket=" + bucket);
		System.err.println("key=" + key);
		System.err.println("hours=" + hours);
		new PreSignUrl(this.s3Client, bucket, key, hours).run();
	}

	private void doStatus (String[] args) {
		if (args.length < 1) {
			usage();
			return;
		}

		String bucket = args[0];
		System.err.println("bucket=" + bucket);
		new Status(this.s3Client, bucket).run();
	}

	private void doClean (String[] args) {
		if (args.length < 1) {
			usage();
			return;
		}

		String bucket = args[0];
		System.err.println("bucket=" + bucket);
		new Clean(this.s3Client, bucket).run();
	}

	private static void findProxy (ClientConfiguration clientConfiguration) throws MalformedURLException {
		String[] envVars = {"https_proxy", "http_proxy"};
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

	public static void main (String[] args) throws Exception {
		new Main().run(args);
	}

}
