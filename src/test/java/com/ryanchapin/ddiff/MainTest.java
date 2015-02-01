package com.ryanchapin.ddiff;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
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
@PrepareForTest(ToolRunner.class)
public class MainTest extends BaseTest{
	
	@Mock
	private DistributedDiff mockDdiff;
	
	@Mock
	private Configuration mockConf;
	
	@Before
	public void setUp() throws Exception {
		mockDdiff  = Mockito.mock(DistributedDiff.class);
		
		PowerMockito.mockStatic(ToolRunner.class);
		Main.setConf(mockConf);
		Main.setDdiff(mockDdiff);
		
		// Set up an invocation of new for the classes that we mock to return
		// the mocked instances for those classes.
		PowerMockito.whenNew(DistributedDiff.class)
			.withNoArguments()
			.thenReturn(mockDdiff);
	}
	
	@After
	public void tearDown() {
		Main.setConf(null);
		Main.setDdiff(null);
		Main.setRetval(0);
	}
	
	@Test
	public void shouldCreateDdiffInstance() throws Exception {
		Main.main(ARGS_VALID);
		PowerMockito.verifyNew(DistributedDiff.class);			
	}

	
	@Test(expected = IllegalStateException.class)
	public void shouldThrowExceptionOnDdiffFailure() throws Exception {
		// Define the mock invocation of ToolRunner.run to return non zero value.
		Mockito.when(ToolRunner.run(mockConf, mockDdiff, ARGS_VALID)).thenReturn(1);
		
		// Make the invocation that we want to test
		Main.main(ARGS_VALID);
	}	
	
	@Test
	public void shouldReturnZeroOnDdiffSuccess() throws Exception {
		// Define the mock invocation of ToolRunner.run to return zero.
		Mockito.when(ToolRunner.run(mockConf, mockDdiff, ARGS_VALID)).thenReturn(0);
		
		// Make the invocation that we want to test
		Main.main(ARGS_VALID);
		
		assertEquals(0, Main.getRetVal());
	}
}
