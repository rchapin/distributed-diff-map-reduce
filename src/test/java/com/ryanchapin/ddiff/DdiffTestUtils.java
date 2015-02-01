package com.ryanchapin.ddiff;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class DdiffTestUtils {

   /**
    * 
    * @param inputRecords
    * @param source
    * @param count        - The number to set for
    *                       {@link TaggedTextWithCountWritableComparable#setCount(IntWritable)}
    * @return
    */
   public static List<MapOutputRecord>
      createMapOutputRecords(List<InputRecord> inputRecords, Source source, int count)
   {
      List<MapOutputRecord> retVal =
         new ArrayList<MapOutputRecord>(inputRecords.size());
   
      for (InputRecord inputRecord : inputRecords) {
         MapOutputRecord outputRecord = new MapOutputRecord();
   
         // Generate the expected output data
         Text key      = new Text(inputRecord.getHash());
         
         Text record          = new Text(inputRecord.getRecord());
         IntWritable countVal = new IntWritable(count);
         Text sourceVal       = new Text(source.toString());
         TaggedTextWithCountWritableComparable value =
               new TaggedTextWithCountWritableComparable(record, sourceVal, countVal);
         
         outputRecord.setKey(key);
         outputRecord.setValue(value);
         
         retVal.add(outputRecord);
      }
      return retVal;
   }
   
   /**
    * Instantiates n number of {@link InputRecord} instances.
    * 
    * @param numRecords - the number of input records to create.
    * @param duplicates - true indicates all of the records created and
    *                     returned will be duplicates.  false indicates that
    *                     they will contain an int suffix that will be
    *                     incremented with each record created.
    * @return           - List of {@link InputRecord} objects.
    */
   public static List<InputRecord> createInputRecords(int numRecords, boolean duplicates) {
      List<InputRecord> inputRecords = new ArrayList<InputRecord>(numRecords);
      
      int counter = 1;
      for (int i = 0; i < numRecords; i++) {
         if (!duplicates) {
            counter++;
         }
         InputRecord inputRecord = 
               new InputRecord(
                     BaseTest.INPUT_RECORD_PREFIX + counter,
                     BaseTest.HASH_PREFIX + counter);
         inputRecords.add(inputRecord);
      }
      return inputRecords;
   }
}
