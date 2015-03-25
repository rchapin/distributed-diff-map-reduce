package com.ryanchapin.ddiff;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
   MultipleInputs.class, MultipleOutputs.class,
   Job.class, FileSystem.class, FileOutputFormat.class
})
public class DistributedDiffTest extends BaseTest {
   
   @Rule
   public final ExpectedSystemExit exit = ExpectedSystemExit.none();
   
   @Mock
   private Path mockRefInputPath;
   
   @Mock
   private Path mockTestInputPath;
   
   @Mock
   private Path mockOutputPath;
   
   @Mock
   private FileSystem mockFileSystem;
   
   @Mock
   protected Configuration mockConf;
    
   @Mock
   protected Job mockJob;
   
   private List<Object> mockList;
   
   public DistributedDiffTest() {
      mockList = new ArrayList<Object>();
   }
   
   @Before
   public void setUp() throws Exception {

      mockConf = Mockito.mock(Configuration.class);
      mockJob  = Mockito.mock(Job.class);
   
      PowerMockito.whenNew(Configuration.class)
         .withNoArguments()
         .thenReturn(mockConf);
      
      PowerMockito.whenNew(Path.class)
         .withArguments(INPUT_PATH_REF_VALID)
         .thenReturn(mockRefInputPath);
      PowerMockito.whenNew(Path.class)
         .withArguments(INPUT_PATH_TEST_VALID)
         .thenReturn(mockTestInputPath);
      PowerMockito.whenNew(Path.class)
         .withArguments(OUTPUT_PATH_VALID)
         .thenReturn(mockOutputPath);
      
      PowerMockito.mockStatic(Job.class);
      Mockito.when(Job.getInstance(mockConf)).thenReturn(mockJob);
      
      PowerMockito.mockStatic(MultipleInputs.class);
      PowerMockito.mockStatic(MultipleOutputs.class);
      PowerMockito.mockStatic(FileOutputFormat.class);
      
      PowerMockito.mockStatic(FileSystem.class);
      Mockito.when(FileSystem.get(mockConf)).thenReturn(mockFileSystem);
      
      mockList.add(mockRefInputPath);
      mockList.add(mockTestInputPath);
      mockList.add(mockOutputPath);
      mockList.add(mockFileSystem);
   }
   
   @After
   public void tearDown() {
      for(@SuppressWarnings("unused") Object mock : mockList) {
         mock = null;
      }
   }
   
   // ------------------------------------------------------------------------
   // Test Methods:
   //    
   
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnNullArgs() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(null);
   }
   
   /** -- Valid Required Input --------------------------------------------- */
   @Test
   public void shouldSetConfigsWithValidInputShortOpts() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_SHORT_OPTS);
      
      assertEquals(INPUT_PATH_REF_VALID,  ddiff.getReferenceInputPath());
      assertEquals(INPUT_PATH_TEST_VALID, ddiff.getTestInputPath());
      assertEquals(OUTPUT_PATH_VALID,     ddiff.getOutputPath());
   }

   @Test
   public void shouldSetConfigsWithValidInputLongOpts() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_LONG_OPTS);
      
      assertEquals(INPUT_PATH_REF_VALID,  ddiff.getReferenceInputPath());
      assertEquals(INPUT_PATH_TEST_VALID, ddiff.getTestInputPath());
      assertEquals(OUTPUT_PATH_VALID,     ddiff.getOutputPath());
   }
   
   @Test
   public void shouldSetConfigurationWithFullSetOfArgs() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_WITH_HASH_ALGO_AND_STRING_ENCODING);
      
      assertEquals(INPUT_PATH_REF_VALID,  ddiff.getReferenceInputPath());
      assertEquals(INPUT_PATH_TEST_VALID, ddiff.getTestInputPath());
      assertEquals(OUTPUT_PATH_VALID,     ddiff.getOutputPath());
      assertEquals(HASH_ALGO_VALID,       ddiff.getHashAlgorithm().toString());
      assertEquals(STRING_ENCODING_VALID, ddiff.getStringEncoding());
   }
   
   /** -- Reference Path Args ---------------------------------------------- */
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpMissingRefInputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_REF_INPUT_PATH_MISSING);
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnEmptyRefInputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_REF_INPUT_PATH_EMPTY);
   }
 
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnNullRefInputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_REF_INPUT_PATH_NULL);
   }
   
   /** -- Test Path Args --------------------------------------------------- */
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnMissingTestInputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_TEST_INPUT_PATH_MISSING);
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnEmptyTestInputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_TEST_INPUT_PATH_EMPTY);
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnNullTestInputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_TEST_INPUT_PATH_NULL);
   }

   /** -- Output Path Args ------------------------------------------------- */
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnMissingOutputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_OUTPUT_PATH_MISSING);
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnEmptyOutputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_OUTPUT_PATH_EMPTY);
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnNullOutputPathArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_OUTPUT_PATH_NULL);
   }
   
   /** -- Job Id Args ------------------------------------------------------ */
   @Test
   public void shouldSetConfigsWithValidInputWithJobIdShortOpt() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_WITH_JOB_ID_SHORT);
      
      assertEquals(JOB_ID, ddiff.getJobId());
   }

   @Test
   public void shouldSetConfigsWithValidInputWithJobIdLongOpt() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_WITH_JOB_ID_LONG);
      
      assertEquals(JOB_ID, ddiff.getJobId());
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnEmptyJobIdArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_JOB_ID_EMPTY);
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnNullJobIdArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_JOB_ID_NULL);
   }
   
   @Test
   public void shouldUseDefaultJobIdWithoutJobIdArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_SHORT_OPTS);
      assertEquals(DistributedDiff.OPTION_JOB_NAME_DEFAULT, ddiff.getJobId());
   }
   
   /** -- Hash Algorithm Args ---------------------------------------------- */
   @Test
   public void shouldUseDefaultHashAlgoOnInvalidHashAlgoArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_INVALID_HASH_ALGO);
      assertEquals(DistributedDiff.OPTION_HASH_ALGO_DEFAULT,
            ddiff.getHashAlgorithm().toString());
   }
   
   @Test
   public void shouldUseDefaultHashAlgoWithoutHashAlgoArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_SHORT_OPTS);
      assertEquals(DistributedDiff.OPTION_HASH_ALGO_DEFAULT,
            ddiff.getHashAlgorithm().toString());
   }

   @Test
   public void shouldUseDefaultHashAlgoWithEmptyHashAlgoArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_HASH_ALGO_EMPTY);
      assertEquals(DistributedDiff.OPTION_HASH_ALGO_DEFAULT,
            ddiff.getHashAlgorithm().toString());
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnNullHashAlgoArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_HASH_ALGO_NULL);
      assertEquals(DistributedDiff.OPTION_HASH_ALGO_DEFAULT,
            ddiff.getHashAlgorithm().toString());
   }
   
   @Test
   public void shouldSetConfigsWithValidInputWithHashAlgoShortOpt() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_HASH_ALGO_SHORT);
      assertEquals(HASH_ALGO_VALID, ddiff.getHashAlgorithm().toString());
   }
   
   @Test
   public void shouldSetConfigsWithValidInputWithHashAlgoLongOpt() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_HASH_ALGO_LONG);
      assertEquals(HASH_ALGO_VALID, ddiff.getHashAlgorithm().toString());   
   }
   
   /** -- String Encoding Args --------------------------------------------- */
   @Test
   public void shouldUseDefaultStringEncodingWithoutStringEncodingArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_SHORT_OPTS);
      assertEquals(DistributedDiff.OPTION_HASH_STRING_ENCODING_DEFAULT, ddiff.getStringEncoding());
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnNullStringEncodingArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_STRING_ENCODING_NULL);
   }

   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnInvalidStringEncodingArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_STRING_ENCODING_INVALID);
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnEmptyStringEncodingArg() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_STRING_ENCODING_EMPTY);
   }
   
   @Test
   public void shouldSetConfigsWithValidInputWithStringEncodingShortOpt() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_WITH_STRING_ENCODING_SHORT);
      assertEquals(STRING_ENCODING_VALID, ddiff.getStringEncoding());
   }
   
   @Test
   public void shouldSetConfigsWithValidInputWithStringEncodingLongOpt() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_WITH_STRING_ENCODING_LONG);
      assertEquals(STRING_ENCODING_VALID, ddiff.getStringEncoding());   
   }
   
   /** -- Help ------------------------------------------------------------- */
   @Test
   public void shouldPrintHelpAndExitWithHelpArg() {
      exit.expectSystemExitWithStatus(0);
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_HELP_SHORT_OPTS);
   }

   @Test
   public void shouldPrintHelpAndExitWithHelpArgLong() {
      exit.expectSystemExitWithStatus(0);
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_HELP_LONG_OPTS);
   }
   
   // ------------------------------------------------------------------------
   // Utility Methods:
   
   private DistributedDiff getConfiguredDdiff() {
      DistributedDiff ddiff = new DistributedDiff();
      ddiff.setConf(mockConf);
      ddiff.setJob(mockJob);
      return ddiff;
   }
}
