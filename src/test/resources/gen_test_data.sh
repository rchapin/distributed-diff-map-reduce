#!/bin/bash

###############################################################################
# Generate test data for the DistributedDiff MR program.
#
# name:     gen_test_data.sh
# author:   Ryan Chapin
# created:  2015-02-01
#
################################################################################
# USAGE:

function about {
   cat << EOF
gen_test_data.sh - Generate test data for the DistributedDiff MR program.  Each 
                   record generated will be a combination of  a  user  supplied
                   string plus and number which will be incremented creating  a
                   set of n number of unique records.
EOF
}

function usage {
   cat << EOF
Usage: getopt-example.sh [OPTIONS] -n num_records
                                   -p record_prefix
                                   -o outfile_name

  -n NUM_RECORDS  The number of records to create

  -p RECORD_PREFIX  A quoted string that will be used as
  
  -o OUTFILE_NAME  File name into which to write the generated data

Options:
  -h HELP
      Outputs this basic usage information.

Example:
   $ gen_test_data.sh -n 2 -p "This is a test" -o out.txt

   Will generate the following data into 'out.txt' in the cwd:

This is a test1
This is a test2

EOF
}

################################################################################

HELP=0

PARSED_OPTIONS=`getopt -o hn:p:o: -- "$@"`

# Check to see if the getopts command failed
if [ $? -ne 0 ];
then
   echo "Failed to parse arguments"
   exit 1
fi

eval set -- "$PARSED_OPTIONS"

# Loop through all of the options with a case statement
while true; do
   case "$1" in
      -h)
         HELP=1
         shift
         ;;

      -n)
         NUM_RECORDS=$2
         shift 2
         ;;

      -p)
         RECORD_PREFIX=$2
         shift 2
         ;;

      -o)
         OUTFILE_NAME=$2
         shift 2
         ;;

      --)
         shift
         break
         ;;
   esac
done

if [ "$HELP" -eq 1 ];
then
   usage
   exit
fi

if [ -z "$NUM_RECORDS" ] || [ -z "$RECORD_PREFIX" ] || [ -z "$OUTFILE_NAME" ];
then
   echo "ERROR: Required arguments were not provided"
   usage
   exit
fi

#
# Delete the OUTFILE_NAME if it already exists
#
if [ -e "$OUTFILE_NAME" ];
then
   rm $OUTFILE_NAME
fi

COUNTER=0

while [ "$COUNTER" -le "$NUM_RECORDS" ];
do

	COUNTER=$((COUNTER + 1))
	echo "$RECORD_PREFIX $COUNTER" >> $OUTFILE_NAME

done

exit 0
