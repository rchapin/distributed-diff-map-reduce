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
import org.junit.Test;
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
   
   // TODO: Flesh out the rest of the testing to check for all of the expected
   //       settings in the Job instance before executing the job.
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnNullArgs() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(null);
   }
   
   @Test(expected = IllegalArgumentException.class)
   public void shouldThrowIllegalArgExcpOnLessThanFourArgs() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_ONLY_THREE_ELEMENTS);
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
      
   @Test
   public void shouldSetConfigsWithValidInput() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID);
      
      assertEquals(INPUT_PATH_REF_VALID,   ddiff.getReferenceInputPath());
      assertEquals(INPUT_PATH_TEST_VALID,  ddiff.getTestInputPath());
      assertEquals(OUTPUT_PATH_VALID, ddiff.getOutputPath());
   }
   
   @Test
   public void shouldSetConfigsWithValidInputWithJobId() {
      DistributedDiff ddiff = getConfiguredDdiff();
      ddiff.run(ARGS_VALID_WITH_JOB_ID);
      
      assertEquals(JOB_ID, ddiff.getJobId());
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
