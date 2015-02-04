package com.ryanchapin.ddiff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ryanchapin.ddiff.DdiffCombiner.DdiffCombinerCounter;

public class DdiffCombinerTest extends BaseTest {
   
   @SuppressWarnings("unused")
   private static final Logger LOGGER = LoggerFactory.getLogger(DdiffCombinerTest.class);
   
   private ReduceDriver<Text,
                        TaggedTextWithCountWritableComparable,
                        Text,
                        TaggedTextWithCountWritableComparable> reduceDriver;
   
   // ------------------------------------------------------------------------
   // Utility Methods:
   //
   
   @Before
   public void setUp() throws Exception {
      reduceDriver = new ReduceDriver<Text,
                                      TaggedTextWithCountWritableComparable,
                                      Text,
                                      TaggedTextWithCountWritableComparable>();
      DdiffCombiner ddiffCombiner = new DdiffCombiner();
      reduceDriver.setReducer(ddiffCombiner);
   }
   
   @After
   public void tearDown() {
      reduceDriver = null;
   }
   
   public void runCombinerTest(Source source,
                              int numDuplicates,
                              Map<DdiffCombinerCounter, Long> expectedCounts) throws IOException
   {
      // Create a single input record that we will use to generate duplicate
      // map output records      
      List<InputRecord> inputRecords = 
            DdiffTestUtils.createInputRecords(numDuplicates, true);
      List<MapOutputRecord> mapOutputRecords =
            DdiffTestUtils.createMapOutputRecords(inputRecords, source, 1);
      
      // Set up the expected output
      InputRecord inputRecord = inputRecords.get(0);
      final Text inputKey = new Text(inputRecord.getHash().toString());
      
      List<TaggedTextWithCountWritableComparable> inputValues =
            new ArrayList<TaggedTextWithCountWritableComparable>(mapOutputRecords.size());
      for (MapOutputRecord mor : mapOutputRecords) {
         inputValues.add(mor.getValue());
      }

      reduceDriver.withInput(inputKey, inputValues);
      
      // We should have one TaggedTextWithCountWritableComparable with a count
      // of numDuplicates
      MapOutputRecord mor = mapOutputRecords.get(0);
      final Text outputKey = new Text(
            mapOutputRecords.get(0).getKey().toString());
      final TaggedTextWithCountWritableComparable outputVal =
            new TaggedTextWithCountWritableComparable(
                  new Text(mor.getValue().getRecord().toString()),
                  new Text(mor.getValue().getSource().toString()),
                  new IntWritable(numDuplicates)
                  );
      
      final Pair<Text, TaggedTextWithCountWritableComparable> output =
            new Pair<Text, TaggedTextWithCountWritableComparable>(outputKey, outputVal);
      
      reduceDriver.addOutput(output);
      reduceDriver.runTest();
      
      Counters counters = reduceDriver.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffCombinerCounter.class);
   }
   
   @Test
   public void shouldAggregateReferenceRecords() throws IOException {
      int numDups = 7;
      Map<DdiffCombinerCounter, Long> expectedCounts =
            new HashMap<DdiffCombinerCounter, Long>();
      expectedCounts.put(DdiffCombinerCounter.REFERENCE_COUNT, (long) numDups);
      expectedCounts.put(DdiffCombinerCounter.TEST_COUNT, 0L);
      
      runCombinerTest(Source.REFERENCE, numDups, expectedCounts);
   }
   
   @Test
   public void shouldAggregateTestRecords() throws IOException {
      int numDups = 13;
      Map<DdiffCombinerCounter, Long> expectedCounts =
            new HashMap<DdiffCombinerCounter, Long>();
      expectedCounts.put(DdiffCombinerCounter.REFERENCE_COUNT, 0L);
      expectedCounts.put(DdiffCombinerCounter.TEST_COUNT, (long) numDups);
      
      runCombinerTest(Source.TEST, numDups, expectedCounts);
   }
}
