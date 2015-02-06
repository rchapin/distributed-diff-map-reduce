package com.ryanchapin.ddiff;

/**
 * Enables the tagging of each record read in map method with the
 * {@link Source#TEST} value.  There are no methods implemented except the
 * constructor.
 */
public class DdiffMapperTestInput extends DdiffMapper {

   // TODO: fix the comment once the interface is sorted out
   /**
    * The constructor for this class exists simply to set the value of the
    * {@link source} member as {@link Source#TEST} and thus tag each
    * record read with that source value in the
    * {@link DdiffMapper#map(org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context)}
    * method.
    */
   public DdiffMapperTestInput() {
      super();
      this.source = Source.TEST;
   }
}
