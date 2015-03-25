package com.ryanchapin.ddiff;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.ryanchapin.ddiff.DdiffMapper.DdiffMapperCounter;
import com.ryanchapin.ddiff.DdiffReducer.DdiffReduceCounter;
import com.ryanchapin.util.HashGenerator;

/**
 * Test cases for the MapReduce classes.
 * 
 * Requires the {@code @PrepareForTest(DdiffReducer.class} annotation to
 * enable the mocking of the MultipleOutputs class.
 *   
 * @author Ryan Chapin
 * @since  2015-01-30
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
   DdiffReducer.class, HashGenerator.class
})
public class DdiffMapperReducerTest extends BaseTest{

   private static final Logger LOGGER = LoggerFactory.getLogger(DdiffMapperReducerTest.class);
   
   private MapDriver<LongWritable,
                     Text, Text,
                     TaggedTextWithCountWritableComparable> mapDriverRef;

   private MapDriver<LongWritable,
                     Text, Text,
                     TaggedTextWithCountWritableComparable> mapDriverTest;
   
   private ReduceDriver<Text,
                        TaggedTextWithCountWritableComparable,
                        Text,
                        IntWritable> reduceDriver;
   
   private MapReduceDriver<LongWritable,
                           Text, Text,
                           TaggedTextWithCountWritableComparable,
                           Text, IntWritable> mapReduceDriver;
   
   // ------------------------------------------------------------------------
   // Utility Methods:
   //
   
   @Before
   public void setUp() throws Exception {
      // Call the mock set-up for this class to reset all of the expectations
      // before each run.
      PowerMockito.mockStatic(HashGenerator.class);
   }
   
   @After
   public void tearDown() {
      mapDriverRef      = null;
      mapDriverTest     = null;
      reduceDriver      = null;
   }
   
   private MapDriver<LongWritable, Text, Text, TaggedTextWithCountWritableComparable>
      setupMapper(Source source, int numRows)
   {
      if (null == source) {
         String errMsg = "source arg to 'setupMapper' was null";
         throw new IllegalArgumentException(errMsg);
      }
      
      DdiffMapper ddiffMapper = null;

      // Create our sample input data
      List<InputRecord> inputRecords = DdiffTestUtils.createInputRecords(numRows, false);
      List<MapOutputRecord> outputRecords = null;
      
      switch (source) {
         case REFERENCE:
            ddiffMapper = new DdiffMapperReferenceInput();
            // Create our expected output data
            outputRecords = DdiffTestUtils.createMapOutputRecords(inputRecords, Source.REFERENCE, 1);
            break;
      
         case TEST:
            ddiffMapper = new DdiffMapperTestInput();
            // Create our expected output data
            outputRecords = DdiffTestUtils.createMapOutputRecords(inputRecords, Source.TEST, 1);
            break;
            
         default:
            break;
      }

      MapDriver<LongWritable, Text, Text, TaggedTextWithCountWritableComparable> mapDriver =
            new MapDriver<LongWritable,
                          Text, Text,
                          TaggedTextWithCountWritableComparable>();
      
      mapDriver.setMapper(ddiffMapper);
      Configuration conf = mapDriver.getConfiguration();
      conf.set(DistributedDiff.CONF_HASH_ALGO_KEY, DdiffMapper.HASH_ALGO_DEFAULT.toString());

      
      // Based on the contents of inputRecords and outputRecords set up
      // all of the mock expectations for the hash generator and add the
      // input and expected output to the mapDriver.
      
      for (int i = 0; i < numRows; i++) {
         
         try {
            Mockito.when(HashGenerator.createHash(
                  inputRecords.get(i).getRecord(), DdiffMapper.ENCODING_DEFAULT, DdiffMapper.HASH_ALGO_DEFAULT))
                  .thenReturn(inputRecords.get(i).getHash()
            );
         } catch (UnsupportedEncodingException |
               IllegalArgumentException |
               NoSuchAlgorithmException e) {
            LOGGER.error("Setting and/or invoking the mock of the static " +
                  " HashGenerator methods threw an exception, " +
                  " e = {}", e.toString());
            e.printStackTrace();
         }
         
         mapDriver.addInput(new LongWritable(i + 1),
               new Text(inputRecords.get(i).getRecord()));
         mapDriver.addOutput(outputRecords.get(i).getKey(),
               outputRecords.get(i).getValue());
      }
      return mapDriver;
   }
   
   private void setUpReducer() {
      reduceDriver = new ReduceDriver<Text,
                                    TaggedTextWithCountWritableComparable,
                                    Text,
                                    IntWritable>();
      DdiffReducer ddiffReducer = new DdiffReducer();
      reduceDriver.setReducer(ddiffReducer);
   }
   
   private void setUpMapReducer(Source source) {
      mapReduceDriver = new MapReduceDriver<
            LongWritable,
            Text, Text,
            TaggedTextWithCountWritableComparable,
            Text, IntWritable>();
      
      DdiffMapper dDiffMapper = null;
      switch (source) {
         case REFERENCE:
            dDiffMapper = new DdiffMapperReferenceInput();
            break;
         case TEST:
            dDiffMapper = new DdiffMapperTestInput();
            break;
      }

      Configuration conf = mapReduceDriver.getConfiguration();
      conf.set(DistributedDiff.CONF_HASH_ALGO_KEY, DdiffMapper.HASH_ALGO_DEFAULT.toString());
      
      mapReduceDriver.withMapper(dDiffMapper);
      mapReduceDriver.withReducer(new DdiffReducer());
   }
   
   private void runMrTest(Source source,
         int numInputRecords,
         String namedOutput,
         Map<DdiffMapperCounter, Long> expectedMapCounts,
         Map<DdiffReduceCounter, Long> expectedReduceCounts) throws IOException
   {
      setUpMapReducer(source);
      
      // Set up the input data
      InputRecord inputRecord = DdiffTestUtils.createInputRecords(1, true).get(0);
      List<Pair<LongWritable, Text>> inputList = new ArrayList<Pair<LongWritable, Text>>();

      // Generate n number of identical records for input
      for (int i = 1; i <= numInputRecords; i++) {
         LongWritable key = new LongWritable(1);
         Text value = new Text(inputRecord.getRecord());
         Pair<LongWritable, Text> pair = new Pair<LongWritable, Text>(key,
               value);
         inputList.add(pair);
      }
      mapReduceDriver.addAll(inputList);

      // Set up the mockHashGenerator to return the expected hash
      try {
         Mockito.when(
               HashGenerator.createHash(inputRecord.getRecord(),
                     DdiffMapper.ENCODING_DEFAULT, DdiffMapper.HASH_ALGO_DEFAULT))
                     .thenReturn(inputRecord.getHash());
      } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
         LOGGER.error("Setting and/or invoking the mock of the static " +
               " HashGenerator methods threw an exception, " +
               " e = {}", e.toString());
         e.printStackTrace();
      }

      // Set up the expected Reduce-side output
      final Text outputKey = new Text(inputRecord.getRecord());
      final IntWritable outputVal = new IntWritable(numInputRecords);
      final Pair<Text, IntWritable> output = new Pair<Text, IntWritable>(
            outputKey, outputVal);
      mapReduceDriver.addMultiOutput(namedOutput, output);

      // The current version of MRUnit (1.1.0) has a bug whereby the Reduce-side
      // results from a mocked MultipleOutputs is not returned to the MRUnit
      // framework and thus the tests always fail. So, as a work-around we will
      // just invoke the MR job and then examine the counters to validate the
      // MR job.
      // mapReduceDriver.runTest();
      mapReduceDriver.run();

      Map<DdiffMapperCounter, Long> mapExpectedCounts = new HashMap<DdiffMapperCounter, Long>();
      mapExpectedCounts.put(DdiffMapperCounter.REFERENCE_COUNT,
            expectedMapCounts.get(DdiffMapperCounter.REFERENCE_COUNT));
      mapExpectedCounts.put(DdiffMapperCounter.TEST_COUNT,
            expectedMapCounts.get(DdiffMapperCounter.TEST_COUNT));

      Map<DdiffReduceCounter, Long> reduceExpectedCounts = new HashMap<DdiffReduceCounter, Long>();
      reduceExpectedCounts.put(DdiffReduceCounter.MISSING,
            expectedReduceCounts.get(DdiffReduceCounter.MISSING));
      reduceExpectedCounts.put(DdiffReduceCounter.EXTRA,
            expectedReduceCounts.get(DdiffReduceCounter.EXTRA));
      reduceExpectedCounts.put(DdiffReduceCounter.REFERENCE_SOURCE,
            expectedReduceCounts.get(DdiffReduceCounter.REFERENCE_SOURCE));
      reduceExpectedCounts.put(DdiffReduceCounter.TEST_SOURCE,
            expectedReduceCounts.get(DdiffReduceCounter.TEST_SOURCE));
      reduceExpectedCounts.put(DdiffReduceCounter.INVALID_SOURCE,
            expectedReduceCounts.get(DdiffReduceCounter.INVALID_SOURCE));

      Counters counters = mapReduceDriver.getCounters();
      DdiffTestUtils.validateCounters(counters, reduceExpectedCounts, DdiffReduceCounter.class);
      DdiffTestUtils.validateCounters(counters, mapExpectedCounts, DdiffMapperCounter.class);
   }
   
   // ------------------------------------------------------------------------
   // Test Methods:
   //   
   
   @Test
   public void shouldGenerateCorrectKeyValuePairsforReferenceMapper() throws IOException {
      int numRows = 5;
      mapDriverRef = setupMapper(Source.REFERENCE, numRows);
      mapDriverRef.run();
      
      List<Pair<Text, TaggedTextWithCountWritableComparable>> output =
            mapDriverRef.getExpectedOutputs();
      for (Pair<Text, TaggedTextWithCountWritableComparable> pair : output) {
         LOGGER.debug(pair.getFirst().toString());
      }
      
      // Set up our expected counts and validate against the actual counts
      Map<DdiffMapperCounter, Long> expectedCounts =
            new HashMap<DdiffMapperCounter, Long>();
      expectedCounts.put(DdiffMapperCounter.REFERENCE_COUNT, (long) numRows );
      expectedCounts.put(DdiffMapperCounter.TEST_COUNT, 0L);
      
      Counters counters = mapDriverRef.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffMapperCounter.class);
   }

   @Test
   public void shouldGenerateCorrectKeyValuePairsforTestMapper() throws IOException {
      int numRows = 6;
      mapDriverTest = setupMapper(Source.TEST, numRows);
      mapDriverTest.runTest();

      // Set up our expected counts and validate against the actual counts
      Map<DdiffMapperCounter, Long> expectedCounts =
            new HashMap<DdiffMapperCounter, Long>();
      expectedCounts.put(DdiffMapperCounter.REFERENCE_COUNT, 0L);
      expectedCounts.put(DdiffMapperCounter.TEST_COUNT, (long) numRows);
      
      Counters counters = mapDriverTest.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffMapperCounter.class);      
   }
   
   @Test
   public void shouldGenerateNoReduceOutput() throws IOException {
      setUpReducer();
      
      // Create a single record and then generate a list that contains two
      // TaggedTextWithCountWritableComparable instances.  There will be one
      // from each source (REFERECE and TEST) and each will have a count of
      // numRecPerSource (2).
      int numInputRecords = 1;
      int numRecPerSource = 2;
      
      List<InputRecord> inputRecords =
            DdiffTestUtils.createInputRecords(numInputRecords, true);
      List<MapOutputRecord> mapOutputRecords =
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.REFERENCE, numRecPerSource);
      mapOutputRecords.addAll(
          DdiffTestUtils.createMapOutputRecords(inputRecords, Source.TEST, numRecPerSource));
      
      final Text key = new Text(inputRecords.get(0).getHash().toString());
      final ImmutableList<TaggedTextWithCountWritableComparable> values =
          ImmutableList.of(
                mapOutputRecords.get(0).getValue(),
                mapOutputRecords.get(1).getValue()
                );
      
      reduceDriver.withInput(key, values);
      
      // Simply run the reduce side of the job, and then we will validate
      // that the counters are zero.  The reason that we do not run .runTest()
      // is because it seems that we cannot set up an expectation for 0 output.
      // Thus, this test relies on the fact that the other tests in this suite
      // are correct and that the counters are also tested for the proper values
      // each time the reduce side is run.
      reduceDriver.run();
     
      Map<DdiffReduceCounter, Long> expectedCounts =
            new HashMap<DdiffReduceCounter, Long>();
      expectedCounts.put(DdiffReduceCounter.MISSING, 0L);
      expectedCounts.put(DdiffReduceCounter.EXTRA, 0L);
      expectedCounts.put(DdiffReduceCounter.REFERENCE_SOURCE, 2L);
      expectedCounts.put(DdiffReduceCounter.TEST_SOURCE, 2L);
      expectedCounts.put(DdiffReduceCounter.INVALID_SOURCE, 0L);
      
      Counters counters = reduceDriver.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffReduceCounter.class);  
   }
   
   @Test
   public void shouldGenerateThreeMissingRecords() throws IOException {
      setUpReducer();
      
      // Create a single record and then generate a list that contains three
      // TaggedTextWithCountWritableComparable instance which is from the
      // REFERENCE source.
      int numInputRecords = 1;
      int numRecPerSource = 3;
      
      List<InputRecord> inputRecords = 
            DdiffTestUtils.createInputRecords(numInputRecords, true);
      
      // Create a List of MapOutputRecords that contains the values that we
      // will pass to the Reducer
      List<MapOutputRecord> mapOutputRecords =
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.REFERENCE, numRecPerSource);

      final Text key = new Text(inputRecords.get(0).getHash().toString());
      final ImmutableList<TaggedTextWithCountWritableComparable> values =
          ImmutableList.of(
                mapOutputRecords.get(0).getValue()
                );
      
      reduceDriver.withInput(key, values);

      // Set up our expected output
      final Text outputKey = new Text(
            mapOutputRecords.get(0).getValue().getRecord().toString());
      final IntWritable outputVal = new IntWritable(numRecPerSource);
      
      final Pair<Text, IntWritable> output =
          new Pair<Text, IntWritable>(outputKey, outputVal);
      reduceDriver.addMultiOutput(DistributedDiff.MISSING_OUTPUT, output);
      reduceDriver.runTest();
      
      Map<DdiffReduceCounter, Long> expectedCounts =
            new HashMap<DdiffReduceCounter, Long>();
      expectedCounts.put(DdiffReduceCounter.MISSING, 3L);
      expectedCounts.put(DdiffReduceCounter.EXTRA, 0L);
      expectedCounts.put(DdiffReduceCounter.REFERENCE_SOURCE, 3L);
      expectedCounts.put(DdiffReduceCounter.TEST_SOURCE, 0L);
      expectedCounts.put(DdiffReduceCounter.INVALID_SOURCE, 0L);
      
      Counters counters = reduceDriver.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffReduceCounter.class);
      
   }
   
   @Test
   public void shouldGenerateTwoExtraRecords() throws IOException {
      setUpReducer();
      
      // Create a single record and then generate a list that contains two
      // TaggedTextWithCountWritableComparable instance which is from the
      // TEST source.
      int numInputRecords = 1;
      int numRecPerSource = 2;
      
      List<InputRecord> inputRecords = 
            DdiffTestUtils.createInputRecords(numInputRecords, true);
      
      // Create a List of MapOutputRecords that contains the values that we
      // will pass to the Reducer
      List<MapOutputRecord> mapOutputRecords =
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.TEST, numRecPerSource);

      final Text key = new Text(inputRecords.get(0).getHash().toString());
      final ImmutableList<TaggedTextWithCountWritableComparable> values =
          ImmutableList.of(
                mapOutputRecords.get(0).getValue()
                );
      
      reduceDriver.withInput(key, values);

      // Set up our expected output
      final Text outputKey = new Text(
            mapOutputRecords.get(0).getValue().getRecord().toString());
      final IntWritable outputVal = new IntWritable(numRecPerSource);
      
      final Pair<Text, IntWritable> output =
          new Pair<Text, IntWritable>(outputKey, outputVal);
      reduceDriver.addMultiOutput(DistributedDiff.EXTRA_OUTPUT, output);
      reduceDriver.runTest(); 
      
      Map<DdiffReduceCounter, Long> expectedCounts =
            new HashMap<DdiffReduceCounter, Long>();
      expectedCounts.put(DdiffReduceCounter.MISSING, 0L);
      expectedCounts.put(DdiffReduceCounter.EXTRA, 2L);
      expectedCounts.put(DdiffReduceCounter.REFERENCE_SOURCE, 0L);
      expectedCounts.put(DdiffReduceCounter.TEST_SOURCE, 2L);
      expectedCounts.put(DdiffReduceCounter.INVALID_SOURCE, 0L);
      
      Counters counters = reduceDriver.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffReduceCounter.class);
   }
   
   /**
    * Tests that the reducer will properly do the subtraction and
    * output the correct number of missing records.
    * @throws IOException 
    */
   @Test
   public void shouldSeeFourRefAndOneTestAndReturnThreeMissing() throws IOException {
      setUpReducer();
      
      // Create a single record and then generate a list that contains four
      // TaggedTextWithCountWritableComparable instances from the
      // REFERENCE source, and one from the TEST source.
      List<InputRecord> inputRecords = 
            DdiffTestUtils.createInputRecords(1, true);
      
      // Create a List of MapOutputRecords that contains the values that we
      // will pass to the Reducer
      List<MapOutputRecord> mapOutputRecords =
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.REFERENCE, 4);
      mapOutputRecords.addAll(
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.TEST, 1)
            );
      
      final Text key = new Text(inputRecords.get(0).getHash().toString());
      final ImmutableList<TaggedTextWithCountWritableComparable> values =
          ImmutableList.of(
                mapOutputRecords.get(0).getValue(),
                mapOutputRecords.get(1).getValue()
                );
      
      reduceDriver.withInput(key, values);
      
      final Text outputKey = new Text(
            mapOutputRecords.get(0).getValue().getRecord().toString());
      // There should be 3 records that have been found missing
      final IntWritable outputVal = new IntWritable(3);
      
      final Pair<Text, IntWritable> output =
            new Pair<Text, IntWritable>(outputKey, outputVal);
      reduceDriver.addMultiOutput(DistributedDiff.MISSING_OUTPUT, output);
      reduceDriver.runTest();
      
      Map<DdiffReduceCounter, Long> expectedCounts =
            new HashMap<DdiffReduceCounter, Long>();
      expectedCounts.put(DdiffReduceCounter.MISSING, 3L);
      expectedCounts.put(DdiffReduceCounter.EXTRA, 0L);
      expectedCounts.put(DdiffReduceCounter.REFERENCE_SOURCE, 4L);
      expectedCounts.put(DdiffReduceCounter.TEST_SOURCE, 1L);
      expectedCounts.put(DdiffReduceCounter.INVALID_SOURCE, 0L);
      
      Counters counters = reduceDriver.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffReduceCounter.class);
   }
   
   /**
    * Tests that the reducer will do the proper subtraction and output the
    * correct number of extra records.
    * @throws IOException
    */
   @Test
   public void shouldSeeOneRefAndFiveTestAndReturnFourExtra() throws IOException {
      setUpReducer();
      
      // Create a single record and then generate a list that contains four
      // TaggedTextWithCountWritableComparable instances from the
      // REFERENCE source, and one from the TEST source.
      List<InputRecord> inputRecords = 
            DdiffTestUtils.createInputRecords(1, true);
      
      // Create a List of MapOutputRecords that contains the values that we
      // will pass to the Reducer
      List<MapOutputRecord> mapOutputRecords =
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.REFERENCE, 1);
      mapOutputRecords.addAll(
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.TEST, 5)
            );
      
      final Text key = new Text(inputRecords.get(0).getHash().toString());
      final ImmutableList<TaggedTextWithCountWritableComparable> values =
          ImmutableList.of(
                mapOutputRecords.get(0).getValue(),
                mapOutputRecords.get(1).getValue()
                );
      
      reduceDriver.withInput(key, values);
      
      final Text outputKey = new Text(
            mapOutputRecords.get(0).getValue().getRecord().toString());
      // There should be 4 records that have been found missing
      final IntWritable outputVal = new IntWritable(4);
      
      final Pair<Text, IntWritable> output =
            new Pair<Text, IntWritable>(outputKey, outputVal);
      reduceDriver.addMultiOutput(DistributedDiff.EXTRA_OUTPUT, output);
      reduceDriver.runTest();
      
      Map<DdiffReduceCounter, Long> expectedCounts =
            new HashMap<DdiffReduceCounter, Long>();
      expectedCounts.put(DdiffReduceCounter.MISSING, 0L);
      expectedCounts.put(DdiffReduceCounter.EXTRA, 4L);
      expectedCounts.put(DdiffReduceCounter.REFERENCE_SOURCE, 1L);
      expectedCounts.put(DdiffReduceCounter.TEST_SOURCE, 5L);
      expectedCounts.put(DdiffReduceCounter.INVALID_SOURCE, 0L);
      
      Counters counters = reduceDriver.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffReduceCounter.class);
   }
   
   /**
    * Input data will consist of three matched records from REFERENCE and
    * TEST as well as a third that does not contain a valid source.
    * 
    * @throws IOException 
    */
   @Test
   public void shouldCountThreeInvalidSourceAndThreeMatchedRecords() throws IOException {
      setUpReducer();
      
      // Create a single record and then generate a list that contains three
      // TaggedTextWithCountWritableComparable instances from each source.
      List<InputRecord> inputRecords = 
            DdiffTestUtils.createInputRecords(1, true);

      // Generate a list that contains one entry with three counts from each
      // source.
      List<MapOutputRecord> mapOutputRecords =
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.REFERENCE, 3);
      mapOutputRecords.addAll(
            DdiffTestUtils.createMapOutputRecords(inputRecords, Source.TEST, 3)
            );
      
      // Generate a record with an invalid source
      TaggedTextWithCountWritableComparable invalidRecord =
            new TaggedTextWithCountWritableComparable();
      invalidRecord.setCount(new IntWritable(3));
      invalidRecord.setRecord(new Text(inputRecords.get(0).getRecord().toString()));
      invalidRecord.setSource(new Text("foo"));
      
      final Text key = new Text(inputRecords.get(0).getHash().toString());
      final ImmutableList<TaggedTextWithCountWritableComparable> values =
          ImmutableList.of(
                mapOutputRecords.get(0).getValue(),
                mapOutputRecords.get(1).getValue(),
                invalidRecord
                );
      
      reduceDriver.withInput(key, values);
      
      // Similar to the shouldGenerateNoReduceOutput() method, since we passing
      // in an equal number of REFERENCE and TEST records we do not set any
      // expected output and just check the counters.
      reduceDriver.run();
      
      Map<DdiffReduceCounter, Long> expectedCounts =
            new HashMap<DdiffReduceCounter, Long>();
      expectedCounts.put(DdiffReduceCounter.MISSING, 0L);
      expectedCounts.put(DdiffReduceCounter.EXTRA, 0L);
      expectedCounts.put(DdiffReduceCounter.REFERENCE_SOURCE, 3L);
      expectedCounts.put(DdiffReduceCounter.TEST_SOURCE, 3L);
      expectedCounts.put(DdiffReduceCounter.INVALID_SOURCE, 3L);
      
      Counters counters = reduceDriver.getCounters();
      DdiffTestUtils.validateCounters(counters, expectedCounts, DdiffReduceCounter.class);
   }
   
   @Test
   public void shouldRunMRandReturnFiveExtra() throws IOException {
      Map<DdiffMapperCounter, Long> expectedMapCounts =
            new HashMap<DdiffMapperCounter, Long>();
      expectedMapCounts.put(DdiffMapperCounter.REFERENCE_COUNT, 0L);
      expectedMapCounts.put(DdiffMapperCounter.TEST_COUNT, 5L);
      
      Map<DdiffReduceCounter, Long> expectedReduceCounts =
            new HashMap<DdiffReduceCounter, Long>();
      expectedReduceCounts.put(DdiffReduceCounter.EXTRA, 5L);
      expectedReduceCounts.put(DdiffReduceCounter.INVALID_SOURCE, 0L);
      expectedReduceCounts.put(DdiffReduceCounter.MISSING, 0L);
      expectedReduceCounts.put(DdiffReduceCounter.REFERENCE_SOURCE, 0L);
      expectedReduceCounts.put(DdiffReduceCounter.TEST_SOURCE, 5L);

      runMrTest(Source.TEST, 5, DistributedDiff.EXTRA_OUTPUT,
            expectedMapCounts, expectedReduceCounts);
   }
   
   @Test
   public void shouldRunMRandReturnThreeMissing() throws IOException {
      Map<DdiffMapperCounter, Long> expectedMapCounts =
            new HashMap<DdiffMapperCounter, Long>();
      expectedMapCounts.put(DdiffMapperCounter.REFERENCE_COUNT, 3L);
      expectedMapCounts.put(DdiffMapperCounter.TEST_COUNT, 0L);
      
      Map<DdiffReduceCounter, Long> expectedReduceCounts =
            new HashMap<DdiffReduceCounter, Long>();
      expectedReduceCounts.put(DdiffReduceCounter.EXTRA, 0L);
      expectedReduceCounts.put(DdiffReduceCounter.INVALID_SOURCE, 0L);
      expectedReduceCounts.put(DdiffReduceCounter.MISSING, 3L);
      expectedReduceCounts.put(DdiffReduceCounter.REFERENCE_SOURCE, 3L);
      expectedReduceCounts.put(DdiffReduceCounter.TEST_SOURCE, 0L);

      runMrTest(Source.REFERENCE, 3, DistributedDiff.MISSING_OUTPUT,
            expectedMapCounts, expectedReduceCounts);
   }
}
