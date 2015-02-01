# A Distributed Differential Tool

**By:** Ryan Chapin [Contact Info](http://www.ryanchapin.com/contact.html)

Distributed-Diff is a utility for comparing large amounts of ASCII text.  Developed to aid in the testing of systems where potentially millions of records could be generated and needing to be able to do a diff against the expected and generated output.

In the case where the file sizes are too large to fit on a single machine, and/or sorting and diffing them is not feasible on a single machine this utility allows the user to compare two different sets of output and determine if there is a matching line for every record in set A (the reference set) in set B (the test output set).

The program will output two sets of records, those that were missing in the test output set, and those additional records in the test output set that should not have been generated.


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

### Running from within Eclipse

There is an eclipse launcher file included in ```src/test/resources/launchers/TDB.launch```.

## Building a Distribution

To build, simply run the following command in the ddiff directory

```
# mvn clean package && mvn assembly:assembly
```

This will build the project and create a distribution .tar.gz file in the target/ directory

## To Run

TBD

