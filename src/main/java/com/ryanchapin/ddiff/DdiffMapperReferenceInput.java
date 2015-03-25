package com.ryanchapin.ddiff;

/**
 * Enables the tagging of each record read in map method with the
 * {@link Source#REFERENCE} value.  There are no methods implemented except the
 * constructor.
 * 
 * @since  1.0.0
 * 
 */
public class DdiffMapperReferenceInput extends DdiffMapper {

   /**
    * The constructor for this class exists simply to set the value of the
    * {@link DdiffMapper#source}  member as {@link Source#REFERENCE} and thus
    * tag each record read with that source value in the
    * {@link DdiffMapper#map(org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context)}
    * method.
    */
   public DdiffMapperReferenceInput() {
      super();
      this.source = Source.REFERENCE;
   }
}
