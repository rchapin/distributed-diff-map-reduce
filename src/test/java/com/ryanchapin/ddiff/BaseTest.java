package com.ryanchapin.ddiff;


public class BaseTest {

	public static final String INPUT_PATH_REF_VALID  = "/user/ddiff/reference_input/";
	public static final String INPUT_PATH_TEST_VALID  = "/user/ddiff/test_input/";
	public static final String OUTPUT_PATH_VALID  = "/user/ddiff/output/";
	public static final String JOB_ID                 = "test_job";
	
	public static final String ARG_EMPTY = "";
	public static final String ARG_JOB_NAME = "ddif_test";
	
	public static final String[] ARGS_VALID =
		{INPUT_PATH_REF_VALID, INPUT_PATH_TEST_VALID, OUTPUT_PATH_VALID};
	
	public static final String[] ARGS_VALID_WITH_JOB_ID =
		{INPUT_PATH_REF_VALID, INPUT_PATH_TEST_VALID, OUTPUT_PATH_VALID, JOB_ID};
	
	public static final String[] ARGS_ONLY_THREE_ELEMENTS = { null, null, null };
	
	public static final String[] ARGS_REF_INPUT_PATH_EMPTY =
		{ARG_EMPTY, INPUT_PATH_TEST_VALID, OUTPUT_PATH_VALID};
	
	public static final String[] ARGS_REF_INPUT_PATH_NULL =
		{null, INPUT_PATH_TEST_VALID, OUTPUT_PATH_VALID};	
	
	public static final String[] ARGS_OUTPUT_PATH_EMPTY =
		{INPUT_PATH_REF_VALID, INPUT_PATH_TEST_VALID, ARG_EMPTY};
	
	public static final String[] ARGS_OUTPUT_PATH_NULL =
		{INPUT_PATH_REF_VALID, INPUT_PATH_TEST_VALID, null};

	public static final String[] ARGS_TEST_INPUT_PATH_EMPTY =
		{INPUT_PATH_REF_VALID, ARG_EMPTY, OUTPUT_PATH_VALID};
	
	public static final String[] ARGS_TEST_INPUT_PATH_NULL =
		{INPUT_PATH_REF_VALID, null, OUTPUT_PATH_VALID};

	public static final String INPUT_RECORD_PREFIX = "This is a record";
	public static final String HASH_PREFIX         = "HASH";
}
