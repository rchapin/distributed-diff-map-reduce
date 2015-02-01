package com.ryanchapin.ddiff;

import org.apache.hadoop.io.Text;

/**
 * Data container class for expected Mapper output records
 * 
 * @since 2015-01-23
 */
public class MapOutputRecord {
   private Text key;
   private TaggedTextWithCountWritableComparable value;
   
   public Text getKey() {
      return key;
   }

   public void setKey(Text key) {
      this.key = key;
   }

   public TaggedTextWithCountWritableComparable getValue() {
      return value;
   }

   public void setValue(TaggedTextWithCountWritableComparable value) {
      this.value = value;
   }

   public MapOutputRecord() {}
   
   public MapOutputRecord(Text key, TaggedTextWithCountWritableComparable value) {        
      this.key   = key;
      this.value = value;
   }
}