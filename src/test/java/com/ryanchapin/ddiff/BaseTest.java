package com.ryanchapin.ddiff;


public class BaseTest {

   public static final String INPUT_PATH_REF_VALID    = "/user/ddiff/reference_input/";
   public static final String INPUT_PATH_TEST_VALID   = "/user/ddiff/test_input/";
   public static final String OUTPUT_PATH_VALID       = "/user/ddiff/output/";
   public static final String JOB_ID                  = "test_job";
   public static final String HASH_ALGO_VALID         = "SHA256SUM";
   public static final String HASH_ALGO_INVALID       = "SHASOMETHINGSUM";
   public static final String STRING_ENCODING_VALID   = "UTF-16";
   public static final String STRING_ENCODING_INVALID = "THIS-IS-INVALID";
   
   public static final String ARG_EMPTY = "";
   public static final String ARG_JOB_NAME = "ddif_test";
   
   
   /** -- Valid Args ------------------------------------------------------- */
   public static final String[] ARGS_VALID_SHORT_OPTS = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID};

   public static final String[] ARGS_VALID_LONG_OPTS = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH_LONG, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH_LONG, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH_LONG, OUTPUT_PATH_VALID};

   public static final String[] ARGS_VALID_WITH_HASH_ALGO_AND_STRING_ENCODING = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_ALGO, HASH_ALGO_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_STRING_ENCODING, STRING_ENCODING_VALID};
   
   /** -- Reference Path Args ---------------------------------------------- */
   public static final String[] ARGS_REF_INPUT_PATH_MISSING = {
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID};
   
   public static final String[] ARGS_REF_INPUT_PATH_EMPTY = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, ARG_EMPTY,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID};
   
   public static final String[] ARGS_REF_INPUT_PATH_NULL = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, null,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID};

   /** -- Test Path Args --------------------------------------------------- */
   public static final String[] ARGS_TEST_INPUT_PATH_MISSING = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID};
   
   public static final String[] ARGS_TEST_INPUT_PATH_EMPTY = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, ARG_EMPTY,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID};
   
   public static final String[] ARGS_TEST_INPUT_PATH_NULL = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, null,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID};
   
   /** -- Output Path Args ------------------------------------------------- */
   public static final String[] ARGS_OUTPUT_PATH_MISSING = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID};
   
   public static final String[] ARGS_OUTPUT_PATH_EMPTY = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, ARG_EMPTY};
   
   public static final String[] ARGS_OUTPUT_PATH_NULL = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, null};

   /** -- Hash Algorithm Args ---------------------------------------------- */
   public static final String[] ARGS_VALID_HASH_ALGO_SHORT = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_ALGO, HASH_ALGO_VALID};
   
   public static final String[] ARGS_VALID_HASH_ALGO_LONG = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH_LONG, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH_LONG, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH_LONG, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_ALGO_LONG, HASH_ALGO_VALID};
   
   public static final String[] ARGS_INVALID_HASH_ALGO = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_ALGO, HASH_ALGO_INVALID };

   public static final String[] ARGS_HASH_ALGO_EMPTY= {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_ALGO, ARG_EMPTY };
   
   public static final String[] ARGS_HASH_ALGO_NULL = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_ALGO, null};
   
   /** -- Job Id Args ------------------------------------------------------ */
   public static final String[] ARGS_VALID_WITH_JOB_ID_SHORT = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_JOB_NAME, JOB_ID};
   
   public static final String[] ARGS_VALID_WITH_JOB_ID_LONG = { 
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH_LONG, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH_LONG, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH_LONG, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_JOB_NAME_LONG, JOB_ID};
   
   public static final String[] ARGS_JOB_ID_EMPTY = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_JOB_NAME, ARG_EMPTY};

   public static final String[] ARGS_JOB_ID_NULL = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_JOB_NAME, null};

   /** -- String Encoding Args --------------------------------------------- */
   public static final String[] ARGS_VALID_WITH_STRING_ENCODING_SHORT = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_STRING_ENCODING, STRING_ENCODING_VALID};

   public static final String[] ARGS_VALID_WITH_STRING_ENCODING_LONG = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_STRING_ENCODING_LONG, STRING_ENCODING_VALID};
   
   public static final String[] ARGS_STRING_ENCODING_EMPTY = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_STRING_ENCODING, ARG_EMPTY};  
   
   public static final String[] ARGS_STRING_ENCODING_NULL = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_STRING_ENCODING, null};
   
   public static final String[] ARGS_STRING_ENCODING_INVALID = {
      "-" + DistributedDiff.OPTION_KEY_REF_INPUT_PATH, INPUT_PATH_REF_VALID,
      "-" + DistributedDiff.OPTION_KEY_TEST_INPUT_PATH, INPUT_PATH_TEST_VALID,
      "-" + DistributedDiff.OPTION_KEY_OUTPUT_PATH, OUTPUT_PATH_VALID,
      "-" + DistributedDiff.OPTION_KEY_HASH_STRING_ENCODING, STRING_ENCODING_INVALID};
   
   public static final String INPUT_RECORD_PREFIX = "This is a record";
   public static final String HASH_PREFIX         = "HASH";
}
