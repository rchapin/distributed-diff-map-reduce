package com.ryanchapin.ddiff;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Will combine any duplicate records emitted from the map method.
 * 
 * @since   1.0.0
 *
 */
public class DdiffCombiner extends Reducer<Text, TaggedTextWithCountWritableComparable, Text, TaggedTextWithCountWritableComparable> {

   @Override
   public void reduce(Text key, Iterable<TaggedTextWithCountWritableComparable> values, Context context)
         throws IOException, InterruptedException
   {
      TaggedTextWithCountWritableComparable value  = null;
      Source source = null;
      int count = 0;
      
      Iterator<TaggedTextWithCountWritableComparable> itr = values.iterator();
      while (itr.hasNext()) {
         value = itr.next();
         count += value.getCount().get();
         if (null == source && null != value) {
            // Only set this once, not for every element in the iterator
            source = Source.valueOf(value.getSource().toString().toUpperCase());
         }
      }
      
      if (count > 1) {
         switch (source) {
            case REFERENCE:
               context.getCounter(DdiffCombinerCounter.REFERENCE_COUNT)
                  .increment((long) count);
               break;
            case TEST:
               context.getCounter(DdiffCombinerCounter.TEST_COUNT)
                  .increment((long) count);
               break;
         }
      }
      
      Text outSource = new Text(value.getSource().toString());
      Text outRecord = new Text(value.getRecord().toString());
      
      TaggedTextWithCountWritableComparable outVal = new TaggedTextWithCountWritableComparable();
      outVal.setSource(outSource);
      outVal.setRecord(outRecord);
      outVal.setCount(new IntWritable(count));
      
      context.write(key, outVal);
   }
   
   public static enum DdiffCombinerCounter {
      REFERENCE_COUNT,
      TEST_COUNT;
   }
}
