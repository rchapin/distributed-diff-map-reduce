package com.ryanchapin.ddiff;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

import com.ryanchapin.util.HashGenerator;
import com.ryanchapin.util.HashGenerator.HashAlgorithm;

/**
 * DistributedDiff is a <b>MRv2</b> utility for comparing large amounts of text
 * data.  As Java, and the [HashGenerator]() library that is used, supports
 * Unicode, DistributedDiff supports a wide range of source data character
 * encodings.  This was developed to aid in the testing of systems where a
 * large volume of records can be generated and where records from a reference
 * set need to be diffed against the output of the system under test.
 * <p>
 * This utility allows the user to compare two different sets of output and
 * determine if there is a matching line for every record in set A
 * (the reference set) in set B (the test output set). It will also determine
 * if there are any additional records in set B (the test output set) that are
 * not in set A (the reference set).
 * <p>
 * The program will output two sets of records, those that were missing in the
 * test output set, and those additional records in the test output set that
 * should not have been generated.
 * <p>
 * It is assumed that each logical record will reside on a single line in both
 * sets of input files.
 * 
 * @since   1.0.0
 *
 */
public class DistributedDiff implements Tool {

   private static Logger LOGGER = LoggerFactory.getLogger(DistributedDiff.class);

   private static final String APP_NAME = "ddiff";
   private static final String REQUIRED = "[required]";
   private static final String OPTIONAL = "[optional]";
   
   /**
    * String to be appended to the output file indicating the records that are
    * missing in the test set, but present in the reference set.
    */
   public static final String MISSING_OUTPUT = "missing";
   
   /**
    * String to be appended to the output file indicating the records that are
    * present in the test set, but are not present in the reference set.
    */
   public static final String EXTRA_OUTPUT   = "extra";
   
   /**
    * Command line interface short option flag for the reference data input
    * path
    */
   public static final String OPTION_KEY_REF_INPUT_PATH       = "r";
   
   /**
    * Command line interface long option flag for the reference data input
    * path
    */
   public static final String OPTION_KEY_REF_INPUT_PATH_LONG  = "reference-data-input-path";
   
   /**
    * Command line interface short option flag for the test data input path
    */
   public static final String OPTION_KEY_TEST_INPUT_PATH      = "t";
   
   /**
    * Command line interface long option flag for the test data input path
    */
   public static final String OPTION_KEY_TEST_INPUT_PATH_LONG = "test-data-input-path";
   
   /**
    * Command line interface short option flag for the output path
    */
   public static final String OPTION_KEY_OUTPUT_PATH      = "o";
   
   /**
    * Command line interface long option flag for the output path
    */
   public static final String OPTION_KEY_OUTPUT_PATH_LONG = "output-path";

   /**
    * Command line interface short option flag for the hash algorithm to be
    * used to hash records.
    */
   public static final String OPTION_KEY_HASH_ALGO      = "a";
   
   /**
    * Command line interface long option flag for the hash algorithm to be
    * used to hash records.
    */
   public static final String OPTION_KEY_HASH_ALGO_LONG = "hash-algorithm";
   
   /**
    * Default hash algorithm to be used.
    */
   public static final String OPTION_HASH_ALGO_DEFAULT  = "SHA1SUM";
   
   /**
    * Command line interface short option flag for the String encoding to be
    * used by the {@link HashGenerator} class when hashing String input.
    */
   public static final String OPTION_KEY_HASH_STRING_ENCODING =
         "e";
   
   /**
    * Command line interface long option flag for the String encoding to be
    * used by the {@link HashGenerator} class when hashing String input.
    */
   public static final String OPTION_KEY_HASH_STRING_ENCODING_LONG =
         "hash-string-encoding";
   
   /**
    * Default String encoding to be used by the {@link HashGenerator} class
    * when hashing String input.
    */
   public static final String OPTION_HASH_STRING_ENCODING_DEFAULT =
         "UTF-8";
   
   /**
    * Command line interface short option flag to provide a user-defined job
    * name.
    */
   public static final String OPTION_KEY_JOB_NAME      = "j";

   /**
    * Command line interface long option flag to provide a user-defined job
    * name.
    */
   public static final String OPTION_KEY_JOB_NAME_LONG = "job-name";
   
   /**
    * Default job name.
    */
   public static final String OPTION_JOB_NAME_DEFAULT  = "ddiff";
   
   /**
    * Command line interface long option flag to print usage/help.
    */
   public static final String OPTION_KEY_HELP_LONG = "help";
   
   /**
    * Command line interface short option flag to print usage/help.
    */
   public static final String OPTION_KEY_HELP = "h";
   
   /**
    * @link Options configured for this program
    */
   private Options options;
   
   /**
    * Key to be used when passing a hash algorithm to the Mappers via the
    * {@link org.apache.hadoop.conf.Configuration} instance.
    */
   public static final String CONF_HASH_ALGO_KEY = "hash.algorithm";
   
   /**
    * Key to be used when passing a String encoding for the {@link HashGenerator}
    * to the Mappers via the {@link org.apache.hadoop.conf.Configuration} instance.
    */
   public static final String CONF_ENCODING_KEY  = "hash.string.encoding";

   /**
    * String array passed in from the {@link com.ryanchapin.ddiff.Main} class.
    */
   private String[] args;
   
   /**
    * Path to the reference input data
    */
   private String referenceInputPath;
   
   /**
    * Path to the test input data.
    */
   private String testInputPath;
   
   /**
    * Path to which the output should be written.
    */
   private String outputPath;

   /**
    * Hash algorithm to be used to hash record keys.
    */
   private HashAlgorithm hashAlgorithm;
   
   /**
    * String encoding to be used for the {@link HashGenerator}
    */
   private String stringEncoding;
   
   /**
    * String to be used for the MapReduce job-id.
    */
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
   
   public HashAlgorithm getHashAlgorithm() {
      return hashAlgorithm;
   }

   public String getStringEncoding() {
      return stringEncoding;
   }
   
   // ------------------------------------------------------------------------
   // Constructor:
   //
   
   public DistributedDiff() {}
   
   // ------------------------------------------------------------------------
   // Member Methods:
   //
   
   /**
    * Receives the String[] args array from the main method which will be
    * parsed by the {@link org.apache.commons.cli} library.
    * 
    * @param args String[] array passed from the main method.
    */
   @Override
   public int run(String[] args) throws IllegalArgumentException {
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
         + "outputPath         :{}\n"
         + "jobId              :{}\n"
         + "hashAlgorithm      :{}",
         referenceInputPath, testInputPath, outputPath, jobId, hashAlgorithm);
      
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
   
   private void parseInputArgs() throws IllegalArgumentException {
      
      // Build our command line options
      @SuppressWarnings("static-access")
      Option refDataPath = OptionBuilder.withLongOpt(OPTION_KEY_REF_INPUT_PATH_LONG)
            .withDescription(REQUIRED + " Input path on HDFS for the reference data")
            .isRequired(true)
            .hasArgs(1)
            .create(OPTION_KEY_REF_INPUT_PATH);

      @SuppressWarnings("static-access")
      Option testDataPath = OptionBuilder.withLongOpt(OPTION_KEY_TEST_INPUT_PATH_LONG)
            .withDescription(REQUIRED + " Input path on HDFS for the test data")
            .isRequired(true)
            .hasArgs(1)
            .create(OPTION_KEY_TEST_INPUT_PATH);
      
      @SuppressWarnings("static-access")
      Option outPath = OptionBuilder.withLongOpt(OPTION_KEY_OUTPUT_PATH_LONG)
            .withDescription(REQUIRED + " Output path on HDFS to where results should be written")
            .isRequired(true)
            .hasArgs(1)
            .create(OPTION_KEY_OUTPUT_PATH);
      
      @SuppressWarnings("static-access")
      Option hashAlgo = OptionBuilder.withLongOpt(OPTION_KEY_HASH_ALGO_LONG)
            .withDescription(OPTIONAL + " Algorithm to be used to hash input records")
            .isRequired(false)
            .hasArgs(1)
            .create(OPTION_KEY_HASH_ALGO);
      
      @SuppressWarnings("static-access")
      Option encoding = OptionBuilder.withLongOpt(OPTION_KEY_HASH_STRING_ENCODING_LONG)
            .withDescription(OPTIONAL + " String encoding to be used when hashing input records")
            .isRequired(false)
            .hasArgs(1)
            .create(OPTION_KEY_HASH_STRING_ENCODING);
      
      @SuppressWarnings("static-access")
      Option jobName = OptionBuilder.withLongOpt(OPTION_KEY_JOB_NAME_LONG)
            .withDescription(OPTIONAL + " User defined name for this M/R job")
            .isRequired(false)
            .hasArgs(1)
            .create(OPTION_KEY_JOB_NAME);
      
      @SuppressWarnings("static-access")
      Option help = OptionBuilder.withLongOpt(OPTION_KEY_HELP_LONG)
            .withDescription("Print this message")
            .isRequired(false)
            .hasArg(false)
            .create(OPTION_KEY_HELP);
      
      options = new Options();
      options.addOption(refDataPath);
      options.addOption(testDataPath);
      options.addOption(outPath);
      options.addOption(hashAlgo);
      options.addOption(encoding);
      options.addOption(jobName);
      options.addOption(help);
      
      // Create the parser and parse the String[] args
      CommandLineParser parser = new BasicParser();
      CommandLine commandLine  = null;
      
      try {
         commandLine = parser.parse(options, args);
         
         if (commandLine.hasOption(OPTION_KEY_HELP) ||
             commandLine.hasOption(OPTION_KEY_HELP_LONG))
         {
            printUsage(true);
         }
         
         referenceInputPath =
               commandLine.getOptionValue(OPTION_KEY_REF_INPUT_PATH);
         LOGGER.info("Cli arg: {} = {}",
               OPTION_KEY_REF_INPUT_PATH_LONG, referenceInputPath);
         
         testInputPath =
               commandLine.getOptionValue(OPTION_KEY_TEST_INPUT_PATH);
         LOGGER.info("Cli arg: {} = {}",
               OPTION_KEY_TEST_INPUT_PATH_LONG, testInputPath);
         
         outputPath = commandLine.getOptionValue(OPTION_KEY_OUTPUT_PATH);
         LOGGER.info("Cli arg: {} = {}", 
               OPTION_KEY_OUTPUT_PATH_LONG, outputPath);
         
         String hashAlgoString = commandLine.getOptionValue(
               OPTION_KEY_HASH_ALGO, OPTION_HASH_ALGO_DEFAULT);
         try {
            hashAlgorithm = HashAlgorithm.valueOf(hashAlgoString.toUpperCase());
            LOGGER.info("Cli arg: {} = {}",
                  OPTION_KEY_HASH_ALGO_LONG, hashAlgorithm.toString());
         } catch (IllegalArgumentException e) {
            String errMsg = "Option " + OPTION_KEY_HASH_ALGO_LONG +
                  " was passed an invalid HashAlgorithm enum, '" +
                  hashAlgoString + ".  Using default value of " +
                  OPTION_HASH_ALGO_DEFAULT;
            LOGGER.error(errMsg + ", e = {}", e.toString());
            
            hashAlgorithm = HashAlgorithm.valueOf(OPTION_HASH_ALGO_DEFAULT);
         }
         
         jobId = commandLine.getOptionValue(
               OPTION_KEY_JOB_NAME, OPTION_JOB_NAME_DEFAULT);
         LOGGER.info("{} is set to {}", OPTION_KEY_JOB_NAME_LONG, jobId);
         
         stringEncoding = commandLine.getOptionValue(
               OPTION_KEY_HASH_STRING_ENCODING,
               OPTION_HASH_STRING_ENCODING_DEFAULT);

         
         LOGGER.info("{} is set to {}",
               OPTION_KEY_HASH_STRING_ENCODING_LONG, stringEncoding);
         
      } catch (ParseException e) {
         String errMsg = "Unable to parse command line properties, e = " + e.toString();
         LOGGER.error(errMsg);
         printUsage(false);
         throw new IllegalArgumentException(errMsg);
      }

      validateArg(referenceInputPath, OPTION_KEY_REF_INPUT_PATH_LONG);
      validateArg(testInputPath,      OPTION_KEY_TEST_INPUT_PATH_LONG);
      validateArg(outputPath,         OPTION_KEY_OUTPUT_PATH_LONG);
      validateArg(jobId,              OPTION_KEY_JOB_NAME_LONG);
      validateArg(stringEncoding,     OPTION_KEY_HASH_STRING_ENCODING_LONG);
      
      // Check to make sure that this is a valid StandardCharsets constant
      Field[] standardCharsetsFields = StandardCharsets.class.getFields();
      boolean validCharSetArg = false;
      for (Field field : standardCharsetsFields) {
         try {
            Charset charset = (Charset) field.get(null);
            if (stringEncoding.equals(charset.displayName())) {
               validCharSetArg = true;
               break;
            }
         } catch (IllegalAccessException e) {
            String errMsg = "Unable to iterate through the Fields of the " +
                  "StandardCharsets class, e = " + e.toString();
            LOGGER.error(errMsg);
            throw new IllegalArgumentException("IllegalAccessException thrown " +
                  "while attempting to validate the " +
                  OPTION_KEY_HASH_STRING_ENCODING_LONG + " argument\n" +
                  "e = " + e.toString());
         }
      }
      if (!validCharSetArg) {
         String errMsg = "Invalid " + OPTION_KEY_HASH_STRING_ENCODING_LONG +
               " argument was provided which is not a valid charset encoding";
         LOGGER.error(errMsg);
         throw new IllegalArgumentException(errMsg);
      }
   }
   
   private void printUsage(boolean exit) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(APP_NAME, options);
      if (exit) {
         System.exit(0); 
      } 
   }
   
   /**
    * Configures the M/R job to be submitted.
    * 
    * @throws Exception
    */
   private void setupJob() throws Exception {
      Configuration conf = getConf();
      
      // Set the hash algorithm to be used in the mappers
      // Make sure to set an key/value pairs that you want to pass in the
      // Configuration BEFORE getting a job instance, as the
      // Job.getInstance(Configuration conf) method makes a COPY of the
      // Configuration instance and does not pass a reference.
      conf.set(CONF_HASH_ALGO_KEY, hashAlgorithm.toString());
      conf.set(CONF_ENCODING_KEY,  stringEncoding);
      
      job = Job.getInstance(conf);
      job.setJarByClass(DistributedDiff.class);

      // Delete the output path if it already exists
      FileSystem fs = FileSystem.get(conf);
      Path outPath = new Path(outputPath);
      if (fs.exists(outPath)) {
         fs.delete(outPath, true);
      }

      job.setJobName(jobId);
   
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      MultipleInputs.addInputPath(
            job, new Path(referenceInputPath),
            TextInputFormat.class, DdiffMapperReferenceInput.class);
      MultipleInputs.addInputPath(
            job, new Path(testInputPath),
            TextInputFormat.class, DdiffMapperTestInput.class);
      
      job.setCombinerClass(DdiffCombiner.class);
      job.setReducerClass(DdiffReducer.class);

      // The only output will be the count of the records that are missing in
      // the reference data, or the count of the additional records in the test
      // data.
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(TaggedTextWithCountWritableComparable.class);
      
      FileOutputFormat.setOutputPath(job, outPath);
      
      MultipleOutputs.addNamedOutput(
            job, MISSING_OUTPUT, TextOutputFormat.class,
            Text.class, IntWritable.class);
      MultipleOutputs.addNamedOutput(
            job, EXTRA_OUTPUT, TextOutputFormat.class,
            Text.class, IntWritable.class); 
   }
   
   private void validateArg(String arg, String argName)
      throws IllegalArgumentException
   {
      if (arg == null || arg.length() == 0) {
         String errMsg = argName + " argument was either null or empty";
         LOGGER.error(errMsg);
         throw new IllegalArgumentException(errMsg);
      }
   }
}
