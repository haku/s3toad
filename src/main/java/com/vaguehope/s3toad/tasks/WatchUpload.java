package com.vaguehope.s3toad.tasks;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.vaguehope.s3toad.util.ExecutorFactory;
import com.vaguehope.s3toad.util.ThreadHelper;

public class WatchUpload {

	protected static final Logger LOG = LoggerFactory.getLogger(WatchUpload.class);

	private final AmazonS3 s3Client;
	private final File dir;
	private final String bucket;
	private final ThreadPoolExecutor controlExecutor;
	private final ThreadPoolExecutor workerExecutor;
	private final long chunkSize;
	private final boolean deleteAfter;

	public WatchUpload(AmazonS3 s3Client, File file, String bucket, int workerThreads, int controlThreads, long chunkSize, boolean deleteAfter) {
		this.s3Client = s3Client;
		this.dir = file;
		this.bucket = bucket;
		this.deleteAfter = deleteAfter;
		this.controlExecutor = ExecutorFactory.newFixedThreadPool("ctrl", controlThreads);
		this.workerExecutor = ExecutorFactory.newFixedThreadPool("wrkr", workerThreads);
		this.chunkSize = chunkSize;
	}

	public void dispose() {
		this.controlExecutor.shutdown();
		this.workerExecutor.shutdown();
	}

	public void run() throws Exception {
		final FileSystemManager fsm = VFS.getManager();
		final FileObject dirObj = fsm.toFileObject(this.dir);

		DefaultFileMonitor fm = new DefaultFileMonitor(new MyFileListener(this));
		fm.setRecursive(true);
		fm.addFile(dirObj);
		fm.start();
		LOG.info("watching={}", this.dir.getAbsolutePath());

		while (true) {
			LOG.info("controlExecutorDepth={} workerExecutorDepth={}", this.controlExecutor.getQueue().size(), this.workerExecutor.getQueue().size());
			ThreadHelper.sleepQuietly(10000L);
		}
	}

	protected void fileCreated(FileChangeEvent event) {
		try {
			final FileObject fileObj = event.getFile();
			final File file = new File(fileObj.getURL().getPath()).getCanonicalFile();

			if (!file.isFile()) {
				LOG.info("Ignoring directory: {}", file.getAbsoluteFile());
				return;
			}

			if (file.getName().toLowerCase().endsWith(".part")) {
				LOG.info("Ignoring .part file: {}", file.getAbsoluteFile());
				return;
			}

			if (file.length() <= 0) {
				LOG.info("Ignoring zero length file: {}", file.getAbsoluteFile());
				return;
			}

			LOG.info("created={}", file.getAbsolutePath());
			final String key = getRelativePath(this.dir, file);
			LOG.info("key={}", key);

			UploadMulti u = new UploadMulti(this.s3Client, file, this.bucket, key, this.workerExecutor, this.chunkSize);
			this.controlExecutor.submit(new UploadCaller(u, this.deleteAfter, this.controlExecutor));
		}
		catch (Exception e) {
			LOG.error("Failed to sechedule upload for created file: {}", event.getFile(), e);
		}
	}

	private static String getRelativePath(File dir, File file) {
		String base = dir.getAbsolutePath();
		String path = file.getAbsolutePath();
		return path.substring(base.length() + (base.endsWith("/") ? 0 : 1));
	}

	private static class UploadCaller implements Callable<Void> {

		private final UploadMulti upload;
		private final boolean deleteAfter;
		private final ExecutorService controlExecutor;

		public UploadCaller(UploadMulti upload, boolean deleteAfter, ExecutorService controlExecutor) {
			this.upload = upload;
			this.deleteAfter = deleteAfter;
			this.controlExecutor = controlExecutor;
		}

		@Override
		public Void call() {
			try {
				this.upload.run();
				if (this.deleteAfter) {
					File file = this.upload.getFile();
					if (file.delete()) {
						LOG.info("deleted={}", file.getAbsolutePath());
					}
					else {
						LOG.error("Failed to delete file: {}", file.getAbsolutePath());
					}
				}
			}
			catch (Exception e) {
				LOG.warn("Upload failed.  It will be rescheduled.", e);
				this.controlExecutor.submit(this);
			}
			return null;
		}

	}

	private static class MyFileListener implements FileListener {

		final private WatchUpload watchUpload;

		public MyFileListener(WatchUpload watchUpload) {
			this.watchUpload = watchUpload;
		}

		@Override
		public void fileCreated(FileChangeEvent event) throws Exception {
			this.watchUpload.fileCreated(event);
		}

		@Override
		public void fileDeleted(FileChangeEvent event) throws Exception {
			// Unused.
		}

		@Override
		public void fileChanged(FileChangeEvent event) throws Exception {
			// Unused.
		}

	}

}
