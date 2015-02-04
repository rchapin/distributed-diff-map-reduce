# A Distributed Differential Tool

**By:** Ryan Chapin [Contact Info](http://www.ryanchapin.com/contact.html)

Distributed-Diff is a **MRv2** utility for comparing large amounts of ASCII text.  Developed to aid in the testing of systems where potentially millions of records could be generated and needing to be able to do a diff against the expected and generated output.

In the case where the file sizes are too large to fit on a single machine, and/or sorting and diffing them is not feasible on a single machine this utility allows the user to compare two different sets of output and determine if there is a matching line for every record in set A (the reference set) in set B (the test output set).

The program will output two sets of records, those that were missing in the test output set, and those additional records in the test output set that should not have been generated.

## Building a Distribution

To build, simply run the following command in the ddiff directory

```
# mvn clean package
```

This will build the project and create a distribution jar in the target/ directory as expected.

If you do not have a valid Hadoop installation on your build machine you will get the following IOException stacktrace:

```
[DEBUG] (main) Shell - Failed to detect a valid hadoop home directory
java.io.IOException: Hadoop home directory ~/temp does not exist, is not a directory, or is not an absolute path.
	at org.apache.hadoop.util.Shell.checkHadoopHome(Shell.java:275)
	at org.apache.hadoop.util.Shell.<clinit>(Shell.java:290)
	at org.apache.hadoop.util.StringUtils.<clinit>(StringUtils.java:76)
	at org.apache.hadoop.conf.Configuration.getStrings(Configuration.java:1689)
	at org.apache.hadoop.io.serializer.SerializationFactory.<init>(SerializationFactory.java:58)
	at org.apache.hadoop.mrunit.internal.io.Serialization.<init>(Serialization.java:39)
	at org.apache.hadoop.mrunit.TestDriver.getSerialization(TestDriver.java:530)
	at org.apache.hadoop.mrunit.TestDriver.copy(TestDriver.java:675)
	at org.apache.hadoop.mrunit.ReduceDriverBase.addInput(ReduceDriverBase.java:167)
	at org.apache.hadoop.mrunit.ReduceDriverBase.withInput(ReduceDriverBase.java:269)
	at com.ryanchapin.ddiff.DdiffMapperReducerTest.shouldGenerateNoReduceOutput(DdiffMapperReducerTest.java:356)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at org.junit.internal.runners.TestMethod.invoke(TestMethod.java:68)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit44RunnerDelegateImpl$PowerMockJUnit44MethodRunner.runTestMethod(PowerMockJUnit44RunnerDelegateImpl.java:310)
	at org.junit.internal.runners.MethodRoadie$2.run(MethodRoadie.java:88)
	at org.junit.internal.runners.MethodRoadie.runBeforesThenTestThenAfters(MethodRoadie.java:96)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit44RunnerDelegateImpl$PowerMockJUnit44MethodRunner.executeTest(PowerMockJUnit44RunnerDelegateImpl.java:294)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit47RunnerDelegateImpl$PowerMockJUnit47MethodRunner.executeTestInSuper(PowerMockJUnit47RunnerDelegateImpl.java:127)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit47RunnerDelegateImpl$PowerMockJUnit47MethodRunner.executeTest(PowerMockJUnit47RunnerDelegateImpl.java:82)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit44RunnerDelegateImpl$PowerMockJUnit44MethodRunner.runBeforesThenTestThenAfters(PowerMockJUnit44RunnerDelegateImpl.java:282)
	at org.junit.internal.runners.MethodRoadie.runTest(MethodRoadie.java:86)
	at org.junit.internal.runners.MethodRoadie.run(MethodRoadie.java:49)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit44RunnerDelegateImpl.invokeTestMethod(PowerMockJUnit44RunnerDelegateImpl.java:207)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit44RunnerDelegateImpl.runMethods(PowerMockJUnit44RunnerDelegateImpl.java:146)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit44RunnerDelegateImpl$1.run(PowerMockJUnit44RunnerDelegateImpl.java:120)
	at org.junit.internal.runners.ClassRoadie.runUnprotected(ClassRoadie.java:33)
	at org.junit.internal.runners.ClassRoadie.runProtected(ClassRoadie.java:45)
	at org.powermock.modules.junit4.internal.impl.PowerMockJUnit44RunnerDelegateImpl.run(PowerMockJUnit44RunnerDelegateImpl.java:122)
	at org.powermock.modules.junit4.common.internal.impl.JUnit4TestSuiteChunkerImpl.run(JUnit4TestSuiteChunkerImpl.java:106)
	at org.powermock.modules.junit4.common.internal.impl.AbstractCommonPowerMockRunner.run(AbstractCommonPowerMockRunner.java:53)
	at org.powermock.modules.junit4.PowerMockRunner.run(PowerMockRunner.java:59)
	at org.apache.maven.surefire.junit4.JUnit4Provider.execute(JUnit4Provider.java:283)
	at org.apache.maven.surefire.junit4.JUnit4Provider.executeWithRerun(JUnit4Provider.java:173)
	at org.apache.maven.surefire.junit4.JUnit4Provider.executeTestSet(JUnit4Provider.java:153)
	at org.apache.maven.surefire.junit4.JUnit4Provider.invoke(JUnit4Provider.java:128)
	at org.apache.maven.surefire.booter.ForkedBooter.invokeProviderInSameClassLoader(ForkedBooter.java:203)
	at org.apache.maven.surefire.booter.ForkedBooter.runSuitesInProcess(ForkedBooter.java:155)
	at org.apache.maven.surefire.booter.ForkedBooter.main(ForkedBooter.java:103)
```

You will still be able to build the jar, but if you want to eliminate the stack-trace, install hadoop/hadoop-mapreduce on your build machine and make sure to set `$HADOP_HOME` environmental variable to point to the hadoop directory that contains the hadoop bin dir.

## To Run

After compilation execute the `yarn jar` command as follows.  The first argument after Main is the input path for the reference data, the second is the input path for the test data and the third is the output path.

$ yarn jar target/distributed-diff-1.0.0.0-SNAPSHOT.jar com.ryanchapin.ddiff.Main /user/rchapin/ddiff/input/ref /user/rchapin/ddiff/input/test /user/rchapin/ddiff/output

To be added is a shell script wrapper to make execution a bit cleaner.

## Development Environment Set-up

This project was developed with Eclipse Kepler, but can be compiled from the command line with maven.

The pom specifies _Java 1.7_.

### To set up to develop, test, and run from within Eclipse (requires m2e and m2e slf4j Eclipse plugins)

1. From within Eclipse, go to _File -> Import_ and select, _Maven -> Existing Maven Projects_ and click _Next_.
2. Then browse to the ddiff/ dir and select it.
3. Set up the log4j configurations:
	- Go to _Window -> Preferences_ and click on _Run/Debug -> String Substitution_.
	- Add the following String variable
		- **DDIFF_LOG_PATH** The fully qualified path to the directory into which you want to write log files while running from eclipse.
	- From within Eclipse, go to _Window -> Preferences -> Java -> Installed JREs_
	- Select your current default 1.7 JRE and copy it
	- Give it some unique name that allows you to associate it with this project.
	- Add the following to the Default VM arguments: `-Dlog.file.path=${DDIFF_LOG_PATH}`
	- Click _Finish_
	- Right-click on the ddiff project in the Package Explorer and select _Build Path -> Configure Build Path_
	- Click on the _Libraries tab_ and then click on the _Add Library_ button
	- Select _JRE System Library_ and then _Next_
	- Select the radio button next to _Alternate JRE:_ and select the ddiff configured JRE
	- Click _Finish_, and then _OK_
4. Configurations to be able to run the M/R job from the eclipse launcher:
	- Go to _Window -> Preferences_ and click on _Run/Debug -> String Substitution_.
	- Add the following String variables
		- **DDIFF_REF_INPUT_DIR** The path to the directory on the *local filesystem* where the REFERENCE input data resides.
		- **DDIFF_TEST_INPUT_DIR** The path to the directory on the *local filesystem* where the TEST input data resides.
		- **DDIFF_OUTPUT_DIR** The path to the directory on the *local filesystem* where the output data will be written.

### Running from within Eclipse

TBD

