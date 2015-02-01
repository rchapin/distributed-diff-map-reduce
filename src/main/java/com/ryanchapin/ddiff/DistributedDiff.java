package com.ryanchapin.ddiff;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for executing a distributed diff against two large data sets.
 * 
 * {@link Main} will instantiate an instance of this class and pass it the
 * String[] args.
 * 
 * @author  Ryan Chapin
 * @since   2015-01-15
 *
 */
public class DistributedDiff implements Tool {

	private static Logger LOGGER = LoggerFactory.getLogger(DistributedDiff.class);

	public static final String HASH_ALGO_KEY = "hash-algo";
	public static final String SOURCE_ID_KEY = "source-id";
	
	public static final String MISSING_OUTPUT = "missing";
	public static final String EXTRA_OUTPUT   = "extra";
	
	private static final int REQUIRED_ARG_COUNT = 3;
	
	private static final String REF_ARG_INPUT_PATH_NAME   = "Reference INPUT PATH Argumen";
	private static final String TEST_ARG_INPUT_PATH_NAME  = "Test INPUT PATH Argument";
	private static final String ARG_OUTPUT_PATH_NANE      = "OUTPUT PATH Argument";
	private static final String JOB_NAME_ARG_NAME         = "Job Name";
	private static final String JOB_NAME_DEFAULT          = "ddiff";
	
	private static final String HASH_ALGO_DEFAULT = "SHA-256";
	
	private String[] args;
	private String referenceInputPath;
	private String testInputPath;
	private String outputPath;
	private String jobId;

	private Job job;
	private Configuration conf;
	
	// ------------------------------------------------------------------------
	// Accessor/Mutators:
	//
	
	public String getReferenceInputPath() {
		return referenceInputPath;
	}

	public void setReferenceInputPath(String referenceInputPath) {
		this.referenceInputPath = referenceInputPath;
	}

	public String getTestInputPath() {
		return testInputPath;
	}

	public void setTestInputPath(String testInputPath) {
		this.testInputPath = testInputPath;
	}
	
	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		if (conf == null) {
			conf = new Configuration();
		}
		return conf;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}	
	
	// ------------------------------------------------------------------------
	// Constructor:
	//
	
	public DistributedDiff() {}
	
	// ------------------------------------------------------------------------
	// Member Methods:
	//
	
	/**
	 * Receives the String[] args array from the main method which should
	 * include the following arguments, in the following order:
	 * <ol>
	 *   <li>Path to Reference Data:  The path on HDFS to the directory that
	 *       contains the reference data that is to be compared.</li>
	 *   <li>Path to the Test Data:  The path on HDFS to the directory that
	 *       contains the data to be compared to the reference data.<li>
	 *   <li>Output Path:  The path on HDFS to the directory to which the
	 *       output will be written.<li>
	 *   <li>Job Name:  An optional String used to create a separate directory
	 *       into which to write the output.  Enables the user to make multiple
	 *       runs on the same input data paths while writing the output to
	 *       different directories</li>
	 * </ol>
	 * @param args String[] array passed from the main method.
	 */
	@Override
	public int run(String[] args) {
		if (args == null) {
			String errMsg = "run method was passed a null String[] args.";
			LOGGER.error(errMsg);
			throw new IllegalArgumentException(errMsg);
		}
		this.args = args;
		parseInputArgs();
		
		LOGGER.info("Invoking DistributedDiff.run with args\n"
			+ "referenceInputPath :{}\n"
			+ "testInputPath      :{}\n"
			+ "outputPath         :{}",
			referenceInputPath, testInputPath, outputPath);
		if (null != jobId && jobId.length() != 0) {
			LOGGER.info("Invoking DistributedDiff.run with arg\n"
				+ "jobId              :{}", jobId);
		}
		
		try {
			setupJob();
			job.submit();
			job.waitForCompletion(true);
		} catch (Exception e) {
			LOGGER.error("Unable to setup, submit or wait for job completion");
			e.printStackTrace();
			return (1);
		}
		
		return (0);
	}
	
	private void parseInputArgs() {
		// Ensure that we have at least 4 arguments.  The 5th is optional
		if (args.length < REQUIRED_ARG_COUNT) {
			String errMsg = "Only " + args.length + " arguments were provided." +
				"  " + REQUIRED_ARG_COUNT + " is the required number of arguments";
			LOGGER.error(errMsg);
			throw new IllegalArgumentException(errMsg);
		}
		
		validateArg(args[0],  REF_ARG_INPUT_PATH_NAME);
		validateArg(args[1],  TEST_ARG_INPUT_PATH_NAME);
		validateArg(args[2],  ARG_OUTPUT_PATH_NANE);
		
		referenceInputPath = args[0];
		testInputPath      = args[1];
		outputPath         = args[2];
		
		if (args.length > REQUIRED_ARG_COUNT) {
			validateArg(args[3], JOB_NAME_ARG_NAME);
			setJobId(args[3]);
		}
	}
	
	private void setupJob() throws Exception {
		Configuration conf = getConf();
		job = Job.getInstance(conf);
		job.setJarByClass(DistributedDiff.class);

		// Delete the output path if it already exists
		FileSystem fs = FileSystem.get(conf);
		Path outPath = new Path(outputPath);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		// Set the hash algorithm to be used in the mappers
		conf.set(HASH_ALGO_KEY, HASH_ALGO_DEFAULT);
		
		String jobName = null;
		if (null != jobId && jobId.length() != 0) {
			jobName = JOB_NAME_DEFAULT + "_" + jobId;
		} else {
			jobName = JOB_NAME_DEFAULT;
		}
		job.setJobName(jobName);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(
		      job, new Path(referenceInputPath),
		      TextInputFormat.class, DdiffMapperReferenceInput.class);
		MultipleInputs.addInputPath(
		      job, new Path(testInputPath),
		      TextInputFormat.class, DdiffMapperTestInput.class);
		
		job.setCombinerClass(DdiffReducer.class);
		job.setReducerClass(DdiffReducer.class);
		// TODO: set-up tests to answer that sof question:
		// job.setPartitionerClass(TaggedKeyPartitioner.class);
		// job.setGroupingComparatorClass(TaggedKeyGroupingComparator.class);
		
		// The only output will be the count of the records that are missing in
		// the reference data, or the count of the additional records in the test
		// data.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job, outPath);
		
		MultipleOutputs.addNamedOutput(
		      job, MISSING_OUTPUT, TextOutputFormat.class,
		      Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(
		      job, EXTRA_OUTPUT, TextOutputFormat.class,
		      Text.class, IntWritable.class); 
	}
	
	private void validateArg(String arg, String argName) {
		if (arg == null || arg.length() == 0) {
			String errMsg = argName + " argument was either null or empty";
			LOGGER.error(errMsg);
			throw new IllegalArgumentException(errMsg);
		}
	}
}
