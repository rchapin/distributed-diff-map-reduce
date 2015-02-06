package com.ryanchapin.ddiff.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashGenerator {
   
   public String createHash(long input, String hashAlgorithm) {
      // Generate a byte array from the long
      // Extract the byte value of the long
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        try {
            dos.writeLong(input);
        } catch (IOException e) {
           e.printStackTrace();
        }
        try {
            dos.flush();
        } catch (IOException e) {
           e.printStackTrace();
        }

        byte[] byteArray = bos.toByteArray();
        return bytesToHex(computeHashBytes(byteArray, hashAlgorithm));
   }
   
   public String createHash(String input, String hashAlgorithm) {
      // Generate a byte array from the input String.
      byte[] inByteArray = null;
      try {
         inByteArray = input.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
         e.printStackTrace();
      }
      
      return bytesToHex(computeHashBytes(inByteArray, hashAlgorithm));
   }
   
   private static byte[] computeHashBytes(byte[] inputBytes, String hashAlgorithm) {
      // Instantiate a MessageDigest instance configured with the desired
      // algorithm.
      MessageDigest md = null;
      try {
         md = MessageDigest.getInstance(hashAlgorithm);
      } catch(NoSuchAlgorithmException e) {
         e.printStackTrace();
      }
      
      // This isn't necessary in this context, but should this
      // be refactored to use the MessageDigest as a member this
      // enables the reuse of the same MessageDigest instance.
      md.reset();
      md.update(inputBytes);
      return md.digest();
   }
   
   private static String bytesToHex(byte[] hashBytes) {
      
      // Convert the hashBytes to a String of hex values
      StringBuilder retVal   = new StringBuilder();
      StringBuilder hexValue = new StringBuilder();
      
      for (byte hashByte : hashBytes) {
         // Flush our StringBuilder to be used as a container for the
         // hex value for each byte as it is read.
         hexValue.delete(0, hexValue.length());
         hexValue.append(Integer.toHexString(0xFF & hashByte));
         
         // Add a trailing '0' if our hexValue is only 1 char long
         if (hexValue.length() == 1) {
            hexValue.insert(0, '0');
         }
         retVal.append(hexValue);
      }
      return retVal.toString();
   }
}
