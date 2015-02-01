package com.ryanchapin.ddiff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WritableComparable to store the record (Text), the source (Text), and the
 * count (IntWritable) of the number of unique records seen.
 *  
 * @author Ryan Chapin
 * @since  2015-01-25
 *
 */
public class TaggedTextWithCountWritableComparable
   implements WritableComparable<TaggedTextWithCountWritableComparable> {

   protected static final Logger LOGGER =
         LoggerFactory.getLogger(TaggedTextWithCountWritableComparable.class);
   
   private IntWritable count;
   private Text source;
   private Text record;
   
   // ------------------------------------------------------------------------
   // Accessor/Mutators
   //   
   public IntWritable getCount() {
      return count;
   }

   public void setCount(IntWritable count) {
      this.count = count;
   }

   public Text getRecord() {
      return record;
   }

   public void setRecord(Text value) {
      this.record = value;
   }

   public Text getSource() {
      return source;
   }

   public void setSource(Text source) {
      this.source = source;
   }
   
   // ------------------------------------------------------------------------
   // Constructor
   //

   public TaggedTextWithCountWritableComparable(Text record, Text source, IntWritable count) {
      this.record  = record;
      this.source = source;
      this.count  = count;
   }
   
   public TaggedTextWithCountWritableComparable() {
      this.record  = new Text();
      this.source = new Text();
      this.count  = new IntWritable();
   }
   
   // ------------------------------------------------------------------------
   // Member Methods
   //

   @Override
   public void write(DataOutput out) throws IOException {
      count.write(out);
      record.write(out);
      source.write(out);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      count.readFields(in);
      record.readFields(in);
      source.readFields(in);
   }

   @Override
   public int compareTo(TaggedTextWithCountWritableComparable o) {
      int retVal = 0;
    
      retVal = this.record.compareTo(o.getRecord());
      if (retVal != 0) { return retVal; }
      
      retVal = this.source.compareTo(o.getSource());
      if (retVal != 0) { return retVal; }
      
      retVal = this.count.compareTo(o.getCount());
      if (retVal != 0) { return retVal; }
      
      return retVal;
   }

   @Override
   public boolean equals(Object o) {
      TaggedTextWithCountWritableComparable other = null;
      try {
         other = (TaggedTextWithCountWritableComparable) o;
      } catch (ClassCastException e) {
         String errMsg = "Object o, passed to equals method was not of type TaggedRecordWithCountWritableComparable<T>";
         LOGGER.error(errMsg);
         return false;
      }
      
      if (this.getRecord().equals(other.getRecord()) &&
          this.getSource().equals(other.getSource()) &&
          this.getCount().equals(other.getCount()))
      {
         return true;
      }
      
      return false;
   }
   
   @Override
   public int hashCode() {
      String compositVal = 
            getRecord().toString() + getSource().toString() + getCount().toString();
      return (compositVal.hashCode());
   }
   
   @Override
   public String toString() {
      return getRecord().toString() + ":" + 
            getSource().toString() + ":" + 
            getCount().toString();
   }
}
