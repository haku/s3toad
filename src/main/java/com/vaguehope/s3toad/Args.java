package com.vaguehope.s3toad;

import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;

public class Args {

	@Argument(index = 0, required = true, metaVar = "<action>", usage = Action.USAGE) private Action action;
	@Argument(index = 1, multiValued = true, metaVar = "ARG") private List<String> args;

	@Option(name = "--threads", aliases = "-t", metaVar = "<count>", usage = "thread count") private int threadCount;
	@Option(name = "--expiry", aliases = "-e", metaVar = "<hours>", usage = "expiry (hours)") private int hours;

	public Action getAction () {
		return this.action;
	}

	public List<String> getArgs (boolean required) throws CmdLineException {
		if (required && (this.args == null || this.args.isEmpty())) throw new CmdLineException(null, "At least one arg is required.");
		return this.args;
	}

	public String getArg (int index, boolean required) throws CmdLineException {
		String value = this.args != null && index < this.args.size() ? this.args.get(index) : null;
		if (required && value == null) throw new CmdLineException(null, "Arg " + index + " is required.");
		return value;
	}

	public void maxArgs (int count) throws CmdLineException {
		if (this.args != null && this.args.size() > count) throw new CmdLineException(null, "Max arg count is  " + count + ".");
	}

	public int getThreadCount (int defVal) {
		return this.threadCount < 1 ? defVal : this.threadCount;
	}

	public int getHours (int defVal) {
		return this.hours < 1 ? defVal : this.hours;
	}

	public static enum Action {
		HELP,
		LIST,
		PUSH,
		PULL,
		URL,
		STATUS,
		CLEAN;
		private static final String USAGE = "" +
				"help\n" +
				"list (bucket)\n" +
				"push [local file path] [bucket]\n" +
				"pull [bucket] [key]\n" +
				"url [bucket] [key]\n" +
				"status [bucket]\n" +
				"clean [bucket]";
	}

}
