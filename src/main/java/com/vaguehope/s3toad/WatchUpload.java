package com.vaguehope.s3toad;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;

import com.amazonaws.services.s3.AmazonS3;

public class WatchUpload {

	private final AmazonS3 s3Client;
	private final File dir;
	private final String bucket;
	private final ExecutorService executor;
	private final long chunkSize;

	public WatchUpload (AmazonS3 s3Client, File file, String bucket, int threads, long chunkSize) {
		this.s3Client = s3Client;
		this.dir = file;
		this.bucket = bucket;
		this.executor = Executors.newFixedThreadPool(threads);
		this.chunkSize = chunkSize;
	}

	public void dispose () {
		this.executor.shutdown();
	}

	public void run () throws Exception {
		final FileSystemManager fsm = VFS.getManager();
		final FileObject dirObj = fsm.toFileObject(this.dir);

		DefaultFileMonitor fm = new DefaultFileMonitor(new MyFileListener(this));
		fm.setRecursive(true);
		fm.addFile(dirObj);
		fm.start();
		System.err.println("watch started.");
		new CountDownLatch(1).await();
	}

	protected void fileCreated (FileChangeEvent event) {
		System.err.println("File changed: " + event);
	}

	private static class MyFileListener implements FileListener {

		final private WatchUpload watchUpload;

		public MyFileListener (WatchUpload watchUpload) {
			this.watchUpload = watchUpload;
		}

		@Override
		public void fileCreated (FileChangeEvent event) throws Exception {
			this.watchUpload.fileCreated(event);
		}

		@Override
		public void fileDeleted (FileChangeEvent event) throws Exception {/* Unused. */}

		@Override
		public void fileChanged (FileChangeEvent event) throws Exception {/* Unused. */}

	}

}
