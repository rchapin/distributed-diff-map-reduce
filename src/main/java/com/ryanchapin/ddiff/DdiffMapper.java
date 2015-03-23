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
   
   protected static final HashAlgorithm HASH_ALGO_DEFAULT = HashAlgorithm.SHA256SUM;
   
   protected static final String ENCODING_DEFAULT = "Unicode";
   
   protected static final IntWritable ONE = new IntWritable(1);
   protected String hashAlgorithm;
   protected Source source;
   protected HashGenerator hashGenerator;
   
   // ------------------------------------------------------------------------
   // Constructor
   //
   
   public HashGenerator getHashGenerator() {
      return hashGenerator;
   }

   public void setHashGenerator(HashGenerator hashGenerator) {
      this.hashGenerator = hashGenerator;
   }

   public DdiffMapper() {
      super();
      hashGenerator = new HashGenerator();
   }
   
   // ------------------------------------------------------------------------
   // Member Methods:
   //
   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      
      Configuration conf = context.getConfiguration();
      String hashAlgoValue = conf.get(DistributedDiff.HASH_ALGO_KEY);

      LOGGER.info("{} value retrieved from Configuration instance = {}",
            DistributedDiff.HASH_ALGO_KEY, hashAlgoValue);
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
