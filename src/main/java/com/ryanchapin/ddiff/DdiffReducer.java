package com.ryanchapin.ddiff;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DdiffReducer extends Reducer<Text, TaggedTextWithCountWritableComparable, Text, IntWritable> {

   private static final Logger LOGGER = LoggerFactory.getLogger(DdiffReducer.class);
   
   private MultipleOutputs<Text, IntWritable> mos;
   
	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	@Override
	protected void reduce(Text key, Iterable<TaggedTextWithCountWritableComparable> values, Context context) throws IOException, InterruptedException {
	   
	   // Separate out the values based on their source
	   Map<Text, Integer> referenceMap = new HashMap<Text, Integer>();
	   Map<Text, Integer> testMap      = new HashMap<Text, Integer>();
	   
	   Source source = null;
	   int count = 0;
	   for (TaggedTextWithCountWritableComparable value : values) {

         count = value.getCount().get();
         
         // Ensure that we don't have some invalid Text value for our
         // Source enum.
	      try {
	         source = Source.valueOf(value.getSource().toString());
	      } catch (IllegalArgumentException e) {
	         String errMsg = "Invalid source value found in reduce record";
	         LOGGER.error(errMsg + ", " + e.toString());
	         long invalidCount = (long) ((count < 1) ? 1 : count);
	         context.getCounter(DdiffReduceCounter.INVALID_SOURCE).increment(invalidCount);
	         continue;
	      }
	      
	      switch (source) {
   	      case REFERENCE:
   	         context.getCounter(DdiffReduceCounter.REFERENCE_SOURCE).increment(count);
   	         upsertMapEntry(value, referenceMap);
   	         break;     
   	      case TEST:
   	         context.getCounter(DdiffReduceCounter.TEST_SOURCE).increment(count);
   	         upsertMapEntry(value, testMap);
   	         break;
   	      default:
	      }
	   }
	   
	   // Now make sure that there is a match in the test set for every record
	   // in the reference set.  We will continue to decrement or remove
	   // items in the test set that we find in the reference set. 
	   Text refKey       = null;
	   Integer refCount  = null;
	   Integer testCount = null;
	   int diff          = 0;
	   
	   Iterator<Map.Entry<Text, Integer>> itr = referenceMap.entrySet().iterator();
	   while (itr.hasNext()) {
	      Map.Entry<Text, Integer> entry = itr.next();
	      
	      // Is there a record in the testMap for this key
         refKey = entry.getKey();
         refCount  = referenceMap.get(refKey);
         
         if (testMap.containsKey(refKey)) {
            testCount = testMap.get(refKey);
            diff = refCount - testCount;
         
            if (diff > 0) {
               // There were missing records in the test set
               mos.write(DistributedDiff.MISSING_OUTPUT, refKey, new IntWritable(diff));
               context.getCounter(DdiffReduceCounter.MISSING).increment(diff);
            } else if (diff < 0) {
               // There were additional records in the test set
               int diffPositive = diff * -1;
               mos.write(DistributedDiff.EXTRA_OUTPUT, refKey, new IntWritable(diffPositive));
               context.getCounter(DdiffReduceCounter.EXTRA).increment(diffPositive);
            }
            
            // Remove the record from the testMap so it will not be included in
            // the set of additional records, since it has just been accounted
            // for
            testMap.remove(refKey);
            
         } else {
            // Add the record and full count to the missing output.
            mos.write(DistributedDiff.MISSING_OUTPUT, refKey, new IntWritable(refCount));
            context.getCounter(DdiffReduceCounter.MISSING).increment(refCount);
         }  
	   }
	   
	   // Now write out the remaining items from the testMap to the extra output
	   for (Map.Entry<Text, Integer> entry : testMap.entrySet()) {
	      mos.write(DistributedDiff.EXTRA_OUTPUT, entry.getKey(), new IntWritable(entry.getValue()));
	      context.getCounter(DdiffReduceCounter.EXTRA).increment((long) entry.getValue());
	   }
	}
	
	private void upsertMapEntry(
	      TaggedTextWithCountWritableComparable value, Map<Text, Integer> map)
	{
	   Text record   = value.getRecord();
      Integer count = map.get(record);
      if (null == count) {
         // Insert a new record with the value from the
         // TaggedTextWithCountWritableComparable indicating the number of
         // records that were found.
         map.put(record, new Integer(value.getCount().get()));
      } else {
         // Increment the count value by the number of records in the Writable
        map.put(record, (count + value.getCount().get()));
      }
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
	
	public static enum DdiffReduceCounter {
	   EXTRA,
	   INVALID_SOURCE,
	   MISSING,
	   REFERENCE_SOURCE,
	   TEST_SOURCE;
	}
}