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
 * Creates a has of the record (the key) and generates a
 * {@link TaggedTextWithCountWritableComparable} instance for each record.
 * 
 * @author Ryan Chapin
 * @since  2015-01-20
 *
 */
public class DdiffMapper extends Mapper<LongWritable, Text, Text, TaggedTextWithCountWritableComparable> {
   
   protected static final Logger LOGGER = LoggerFactory.getLogger(DdiffMapper.class);
   
   public static final HashAlgorithm HASH_ALGO_DEFAULT = HashAlgorithm.SHA256SUM;
   
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
      
      Configuration conf = context.getConfiguration();
//      hashAlgorithm = Enum.valueOf(HashAlgorithm.class, conf.get(DistributedDiff.CONF_HASH_ALGO_KEY));
      String hashAlgo = conf.get(DistributedDiff.CONF_HASH_ALGO_KEY);
      System.out.printf("hashAlgo = %s%n", hashAlgo);
      hashAlgorithm = HashAlgorithm.valueOf(conf.get(DistributedDiff.CONF_HASH_ALGO_KEY));
      stringEncoding = conf.get(DistributedDiff.CONF_ENCODING_KEY);
      
      // TODO: Update log message
      LOGGER.info("{} value retrieved from Configuration instance = {}",
            DistributedDiff.CONF_HASH_ALGO_KEY, hashAlgorithm.toString());
   }
   
   /**
    * Takes each line from the source file, hashes the value, and then creates
    * a composite key of the hash of the value and the source, and a composite
    * value which is the record and it's count.
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
         hashKey = HashGenerator.createHash(value.toString(), ENCODING_DEFAULT, HASH_ALGO_DEFAULT);
      } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
         LOGGER.error("Exception thrown when attempting to hash the input key, e = {}",
               e.getCause().getMessage());
         e.printStackTrace();
      }
      Text outKey    = new Text(hashKey);
      
      TaggedTextWithCountWritableComparable outVal =
            new TaggedTextWithCountWritableComparable(value, new Text(source.toString()), ONE);

      context.write(outKey, outVal);
      
      switch (source) {
         case REFERENCE:
            context.getCounter(DdiffMapperCounter.REFERENCE_COUNT).increment(1L);
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
