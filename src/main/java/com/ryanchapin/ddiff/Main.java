package com.ryanchapin.ddiff;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Entry point for the DistributedDiff program.
 * 
 * @author  Ryan Chapin
 * @since   2015-01-15
 */
public class Main {

	private static Configuration conf = new Configuration();
	private static DistributedDiff ddiff = new DistributedDiff();
	private static int retval;
	
	// ------------------------------------------------------------------------
	// Accessor/Mutators:
	//

	public static Configuration getConf() {
		return conf;
	}

	public static void setConf(Configuration conf) {
		Main.conf = conf;
	}
		
	public static void setDdiff(DistributedDiff launcher) {
		Main.ddiff = launcher;
	}

	public static DistributedDiff getDdiff() {
		return ddiff;
	}
	
	public static int getRetVal() {
		return retval;
	}
	
	public static void setRetval(int retVal) {
		Main.retval = retVal;
	}
	
	// ------------------------------------------------------------------------
	// main:
	//	
	
	public static void main(String[] args) throws Exception {
		int retVal = ToolRunner.run(conf, ddiff, args);
		Main.setRetval(retVal);

		if (Main.getRetVal() != 0) {
			throw new IllegalStateException();
		}
	}
}
