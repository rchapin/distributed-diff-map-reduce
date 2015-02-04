package com.ryanchapin.ddiff;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DdiffTestUtils {

   private static final Logger LOGGER = LoggerFactory.getLogger(DdiffTestUtils.class);
   
   /**
    * Instantiates a {@link MapOutputRecord} for every instance of
    * an {@link InputRecord} contained in the inputRecords argument.
    * 
    * @param inputRecords - List of {@link InputRecords} from which {@link MapOutputRecord}
    *                       instances will be generated
    * @param source       - The {@link Source} enum value to indicate the input data source.
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
   
   public static <E extends Enum<E>> Map<E, Long> getCounters(Counters counters, Class<E> enumType) {
      Map<E, Long> retVal = new HashMap<E, Long>();
      long   count = 0;
      
      E[] values = enumType.getEnumConstants();
      for (E value : values) {
         LOGGER.debug("value = {}", value.toString());
         count = counters.findCounter(value).getValue();
         retVal.put(value, count);
      }
      return retVal;
   }

   public static <E extends Enum<E>> void validateCounters(
         Counters counters, Map<E, Long> expected, Class<E> enumType)
   {
      // Get the map of our counters
      Map<E, Long> actualCounters = getCounters(counters, enumType);
      
      // We could simply invoke a .equals or .compare but if we iterate through
      // the expected map and check each value, we can then output some feedback
      // as to what has or has not matched
     
      E expectedKey    = null;
      long expectedVal = 0;
      long actualVal   = 0;
      for (Map.Entry<E, Long> entry : expected.entrySet()) {
         expectedKey = entry.getKey();
         expectedVal = entry.getValue();
         actualVal   = actualCounters.get(expectedKey);
         
         LOGGER.debug("expectedKey = {}, expectedVal = {}, actualVal = {}",
               expectedKey, expectedVal, actualVal);
         assertEquals("Expected counter value of " + expectedVal + " for " +
               expectedKey.toString() + ":", expectedVal, actualVal);
         
         // Reset the actualVal
         actualVal = 0;
      }
   }
   
}
