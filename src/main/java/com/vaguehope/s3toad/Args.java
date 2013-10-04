package com.vaguehope.s3toad;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;

public class Args {

	@Argument(index = 0, required = true, metaVar = "<action>", usage = Action.USAGE) private Action action;
	@Argument(index = 1, multiValued = true, metaVar = "ARG") private List<String> args;

	@Option(name = "--chunksize", aliases = "-s", metaVar = "<count>", usage = "chunk size (bytes)") private long chunkSize;
	@Option(name = "--threads", aliases = "-t", metaVar = "<count>", usage = "thread count") private int threadCount;
	@Option(name = "--controls", aliases = "-c", metaVar = "<count>", usage = "control thread count") private int controlCount;
	@Option(name = "--expiry", aliases = "-e", metaVar = "<hours>", usage = "expiry (hours)") private int hours;
	@Option(name = "--delete", usage = "delete files after upload") private boolean delete;
    @Option(name = "--metadata", aliases = "-m", metaVar = "<metadata>", usage = "key=value metadata to add to files when uploading/copying, can be specified multiple times", multiValued = true) private List<String> metadata;

	public Action getAction () {
		return this.action;
	}

	public List<String> getArgs (final boolean required) throws CmdLineException {
		if (required && (this.args == null || this.args.isEmpty())) throw new CmdLineException(null, "At least one arg is required.");
		return this.args;
	}

	public String getArg (final int index, final boolean required) throws CmdLineException {
		String value = this.args != null && index < this.args.size() ? this.args.get(index) : null;
		if (required && value == null) throw new CmdLineException(null, "Arg " + index + " is required.");
		return value;
	}

	public void minArgs (final int count) throws CmdLineException {
		if (this.args != null && this.args.size() <count) throw new CmdLineException(null, "Min arg count is  " + count + ", found " + this.args.size() + ".");
	}

	public void maxArgs (final int count) throws CmdLineException {
		if (this.args != null && this.args.size() > count) throw new CmdLineException(null, "Max arg count is  " + count + ", found " + this.args.size() + ".");
	}

	public int getThreadCount (final int defVal) {
		return this.threadCount < 1 ? defVal : this.threadCount;
	}

	public int getControlThreads (final int defVal) {
		return this.controlCount < 1 ? defVal : this.controlCount;
	}

	public long getChunkSize(final long defVal) {
		return this.chunkSize < 1 ? defVal : this.chunkSize;
	}

	public int getHours (final int defVal) {
		return this.hours < 1 ? defVal : this.hours;
	}

    public Map<String, String> getMetadata() throws CmdLineException {
        Map<String, String> result = new LinkedHashMap<String, String>();

        for (String s : metadata) {
            String[] parts = s.split("=", 2);
            if (parts.length != 2) throw new CmdLineException(null, "Faied to parse metadata as a key=value pair: " + s);
            result.put(parts[0], parts[1]);
        }

        return result;
    }

	public boolean getDelete () {
		return this.delete;
	}

	public static enum Action {
		HELP,
		LIST,
		PUSH,
		PULL,
		WATCH,
		URL,
		STATUS,
		CLEAN,
		COPY,
		EMPTY,
        METADATA,
        ABORT_UPLOAD,
        ;
		private static final String USAGE = "" +
				"help\n" +
				"list (bucket)\n" +
				"push [local file path] [bucket]\n" +
				"watch [local dir path] [bucket]\n" +
				"pull [bucket] [key]\n" +
				"url [bucket] [key]\n" +
				"status [bucket]\n" +
				"clean [bucket]\n" +
				"copy [sourceBucket] [sourceKey] [destinationBucket] [destinationKey]\n" +
				"empty [bucket]" +
                "abort_upload [bucket] [key] [id]" +
                "metadata [bucket] [key]"
                ;
	}

}
