package com.ryanchapin.ddiff;

/**
 * Data container class for sample input records
 * 
 * @since 2015-01-23
 */
public class InputRecord {
   private String record;
   private String hash;
   
   public String getRecord() {
      return record;
   }

   public void setRecord(String record) {
      this.record = record;
   }

   public String getHash() {
      return hash;
   }

   public void setHash(String hash) {
      this.hash = hash;
   }

   public InputRecord(String record, String hash) {
      this.record = record;
      this.hash   = hash;
   }
}
