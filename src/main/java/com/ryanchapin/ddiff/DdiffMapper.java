package com.ryanchapin.ddiff;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ryanchapin.util.HashGenerator;
import com.ryanchapin.util.HashGenerator.HashAlgorithm;

/**
 * Reads input files, line by line, creates a hash of the records and then
 * emits keys as the hash of the records, and values as a
 * {@link TaggedTextWithCountWritableComparable} instance for each record.
 * 
 * @since  1.0.0
 */
public class DdiffMapper extends Mapper<LongWritable, Text, Text, TaggedTextWithCountWritableComparable> {
   
   protected static final Logger LOGGER = LoggerFactory.getLogger(DdiffMapper.class);
   
   /**
    * Hash algorithm to be used when generating keys for each record with the
    * {@link HashGenerator} class.
    */
   public static final HashAlgorithm HASH_ALGO_DEFAULT = HashAlgorithm.SHA256SUM;
   
   /**
    * Default String encoding to be passed to
    * {@link HashGenerator#createHash(String, String, HashAlgorithm)}
    */
   public static final String ENCODING_DEFAULT = "UTF-8";
   
   protected static final IntWritable ONE = new IntWritable(1);
   protected HashAlgorithm hashAlgorithm;
   protected String stringEncoding;
   protected Source source;
   
   // ------------------------------------------------------------------------
   // Accessor/Mutators:
   //
   
   public HashAlgorithm getHashAlgorithm() {
      return hashAlgorithm;
   }

   public String getStringEncoding() {
      return stringEncoding;
   }
   
   // ------------------------------------------------------------------------
   // Constructor
   //
     
   public DdiffMapper() {
      super();
   }
   
   // ------------------------------------------------------------------------
   // Member Methods:
   //
   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      
      // Read our configuration data from the Configuration instance
      Configuration conf = context.getConfiguration();
      hashAlgorithm = Enum.valueOf(HashAlgorithm.class,
            conf.get(DistributedDiff.CONF_HASH_ALGO_KEY));
      stringEncoding = conf.get(DistributedDiff.CONF_ENCODING_KEY);

      LOGGER.info("Values from Configuration instance\n\t{} = {}\n\t{} = {}",
            DistributedDiff.CONF_HASH_ALGO_KEY, hashAlgorithm.toString(),
            DistributedDiff.CONF_ENCODING_KEY, stringEncoding);
   }
   
   /**
    * Takes each line from the source file, hashes the value, and then creates
    * a key with that hash and an output value that is a
    * {@link TaggedTextWithCountWritableComparable} instance which is the
    * record and it's count.
    * 
    * @throws InterruptedException 
    * @throws IOException 
    */
   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException
         
   {
      String hashKey = null;
      try {
         hashKey = HashGenerator.createHash(value.toString(),
               ENCODING_DEFAULT, HASH_ALGO_DEFAULT);
      } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
         // Catch an exception that might be thrown directly by the HashGenerator,
         // log the error and then throw an un-checked exception to cause the
         // mapper to fail, as we cannot continue processing with null keys.
         String errMsg = "Exception thrown from HashGenerator when attempting " +
               " to hash the input key, e = " + e.getCause().getMessage();
         LOGGER.error(errMsg);
         e.printStackTrace();
         throw new IllegalStateException(errMsg);
      }
      
      Text outKey = new Text(hashKey);
      TaggedTextWithCountWritableComparable outVal =
            new TaggedTextWithCountWritableComparable(
                  value, new Text(source.toString()), ONE);

      context.write(outKey, outVal);
      
      switch (source) {
         case REFERENCE:
            context.getCounter(DdiffMapperCounter.REFERENCE_COUNT)
               .increment(1L);
            break;
         case TEST:
            context.getCounter(DdiffMapperCounter.TEST_COUNT).increment(1L);
            break;
      }
   }
   
   public static enum DdiffMapperCounter {
      REFERENCE_COUNT,
      TEST_COUNT;
   }
}
