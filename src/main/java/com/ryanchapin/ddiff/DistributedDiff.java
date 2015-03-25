package com.ryanchapin.ddiff;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
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

import com.ryanchapin.util.HashGenerator.HashAlgorithm;

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

   public static final String MISSING_OUTPUT = "missing";
   public static final String EXTRA_OUTPUT   = "extra";
   
   public static final String OPTION_KEY_REF_INPUT_PATH       = "r";
   public static final String OPTION_KEY_REF_INPUT_PATH_LONG  = "reference-data-input-path";
   
   public static final String OPTION_KEY_TEST_INPUT_PATH      = "t";
   public static final String OPTION_KEY_TEST_INPUT_PATH_LONG = "test-data-input-path";
   
   public static final String OPTION_KEY_OUTPUT_PATH      = "o";
   public static final String OPTION_KEY_OUTPUT_PATH_LONG = "output-path";

   public static final String OPTION_KEY_HASH_ALGO      = "a";
   public static final String OPTION_KEY_HASH_ALGO_LONG = "hash-algorithm";
   public static final String OPTION_HASH_ALGO_DEFAULT  = "SHA1SUM";
   
   public static final String OPTION_KEY_HASH_STRING_ENCODING =
         "e";
   public static final String OPTION_KEY_HASH_STRING_ENCODING_LONG =
         "hash-string-encoding";
   public static final String OPTION_HASH_STRING_ENCODING_DEFAULT =
         "UTF-8";
   
   public static final String OPTION_KEY_JOB_NAME      = "j";
   public static final String OPTION_KEY_JOB_NAME_LONG = "job-name";
   public static final String OPTION_JOB_NAME_DEFAULT  = "ddiff";
   
   public static final String CONF_HASH_ALGO_KEY = "hash-algorithm";
   public static final String CONF_ENCODING_KEY  = "hash-string-encoding";

   private String[] args;
   private String referenceInputPath;
   private String testInputPath;
   private String outputPath;
   private HashAlgorithm hashAlgorithm;
   private String stringEncoding;
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
            .withDescription("Input path on HDFS for the reference data")
            .isRequired(true)
            .hasArgs(1)
            .create(OPTION_KEY_REF_INPUT_PATH);

      @SuppressWarnings("static-access")
      Option testDataPath = OptionBuilder.withLongOpt(OPTION_KEY_TEST_INPUT_PATH_LONG)
            .withDescription("Input path on HDFS for the test data")
            .isRequired(true)
            .hasArgs(1)
            .create(OPTION_KEY_TEST_INPUT_PATH);
      
      @SuppressWarnings("static-access")
      Option outPath = OptionBuilder.withLongOpt(OPTION_KEY_OUTPUT_PATH_LONG)
            .withDescription("Output path on HDFS to where results should be written")
            .isRequired(true)
            .hasArgs(1)
            .create(OPTION_KEY_OUTPUT_PATH);
      
      @SuppressWarnings("static-access")
      Option hashAlgo = OptionBuilder.withLongOpt(OPTION_KEY_HASH_ALGO_LONG)
            .withDescription("Algorithm to be used to hash input records")
            .isRequired(false)
            .hasArgs(1)
            .create(OPTION_KEY_HASH_ALGO);
      
      @SuppressWarnings("static-access")
      Option encoding = OptionBuilder.withLongOpt(OPTION_KEY_HASH_STRING_ENCODING_LONG)
            .withDescription("String encoding to be used when hashing input records")
            .isRequired(false)
            .hasArgs(1)
            .create(OPTION_KEY_HASH_STRING_ENCODING);
      
      @SuppressWarnings("static-access")
      Option jobName = OptionBuilder.withLongOpt(OPTION_KEY_JOB_NAME_LONG)
            .withDescription("User defined name for this M/R job")
            .isRequired(false)
            .hasArgs(1)
            .create(OPTION_KEY_JOB_NAME);
      
      Options options = new Options();
      options.addOption(refDataPath);
      options.addOption(testDataPath);
      options.addOption(outPath);
      options.addOption(hashAlgo);
      options.addOption(encoding);
      options.addOption(jobName);
      
      // Create the parser and parse the String[] args
      CommandLineParser parser = new BasicParser();
      CommandLine commandLine  = null;
      
      try {
         commandLine = parser.parse(options, args);
         
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
      conf.set(CONF_HASH_ALGO_KEY, hashAlgorithm.toString());
      conf.set(CONF_ENCODING_KEY,  stringEncoding);
      
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
