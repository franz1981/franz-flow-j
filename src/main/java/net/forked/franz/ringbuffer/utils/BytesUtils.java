/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.forked.franz.ringbuffer.utils;

import java.nio.ByteBuffer;

import sun.misc.Unsafe;

public final class BytesUtils {

   /**
    * Length of the CPU cache line in bytes.
    */
   public static final int CACHE_LINE_LENGTH = 64;
   public static final int PAGE_SIZE = UnsafeAccess.UNSAFE.pageSize();
   private static final long BYTE_BUFFER_HB_FIELD_OFFSET;
   private static final long BYTE_BUFFER_OFFSET_FIELD_OFFSET;

   static {
      try {
         BYTE_BUFFER_HB_FIELD_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
         BYTE_BUFFER_OFFSET_FIELD_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(ByteBuffer.class.getDeclaredField("offset"));
      } catch (final Exception ex) {
         throw new RuntimeException(ex);
      }
   }

   private BytesUtils() {
   }

   public static long align(final long value, final long pow2alignment) {
      return (value + (pow2alignment - 1)) & ~(pow2alignment - 1);
   }

   /**
    * Get the address at which the underlying buffer storage begins.
    *
    * @param buffer that wraps the underlying storage.
    * @return the memory address at which the buffer storage begins.
    */
   public static long address(final ByteBuffer buffer) {
      return ((sun.nio.ch.DirectBuffer) buffer).address();
   }

   /**
    * Get the array from a read-only {@link ByteBuffer}.
    *
    * @param buffer that wraps the underlying array.
    * @return the underlying array.
    */
   public static byte[] array(final ByteBuffer buffer) {
      return (byte[]) UnsafeAccess.UNSAFE.getObject(buffer, BYTE_BUFFER_HB_FIELD_OFFSET);
   }

   /**
    * Get the array offset from a read-only {@link ByteBuffer}.
    *
    * @param buffer that wraps the underlying array.
    * @return the underlying array offset at which this ByteBuffer starts.
    */
   public static int arrayOffset(final ByteBuffer buffer) {
      return UnsafeAccess.UNSAFE.getInt(buffer, BYTE_BUFFER_OFFSET_FIELD_OFFSET);
   }

   public static int alignedIndex(ByteBuffer directByteBuffer, int index, int pow2alignment) {
      if (!isPowOf2(pow2alignment)) {
         throw new IllegalArgumentException("Alignment must be a power of 2");
      }
      final long address = address(directByteBuffer) + index;
      final long alignedAddress = BytesUtils.align(address, pow2alignment);
      final int newIndex = (int) (alignedAddress - address);
      return newIndex;
   }

   /**
    * Test if a value is pow2alignment-aligned.
    *
    * @param value         to be tested.
    * @param pow2alignment boundary the address is tested against.
    * @return true if the address is on the aligned boundary otherwise false.
    * @throws IllegalArgumentException if the alignment is not a power of 2
    */
   public static boolean isAligned(final long value, final int pow2alignment) {
      if (!isPowOf2(pow2alignment)) {
         throw new IllegalArgumentException("Alignment must be a power of 2");
      }
      return (value & (pow2alignment - 1)) == 0;
   }

   public static void zeros(final ByteBuffer buffer) {
      zeros(buffer, buffer.position(), buffer.remaining());
   }

   public static void zeros(final ByteBuffer buffer, final int offset, final int length) {
      long address;
      Object obj;
      if (buffer.isDirect()) {
         address = address(buffer) + offset;
         obj = null;
      } else {
         address = Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset(buffer) + offset;
         obj = array(buffer);
      }
      UnsafeAccess.UNSAFE.setMemory(obj, address, length, (byte) 0);
   }

   /**
    * Is a value a positive power of two.
    *
    * @param value to be checked.
    * @return true if the number is a positive power of two otherwise false.
    */
   public static boolean isPowOf2(final int value) {
      return Integer.bitCount(value) == 1;
   }

   /**
    * Fast method of finding the next power of 2 greater than or equal to the given value.
    *
    * If the value is &lt;= 0 then 1 will be returned.
    *
    * This method is not suitable for {@link Integer#MIN_VALUE} or numbers greater than 2^30.
    *
    * @param value from which to search for next power of 2
    * @return The next power of 2 or the value itself if it is a power of 2
    */
   public static int nextPowOf2(final int value) {
      return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
   }

   public static void copy(ByteBuffer srcBuffer, int srcIndex, Object dstObj, long dstAddress, int length) {
      final Object srcObj;
      final long srcAddress;
      if (srcBuffer.isDirect()) {
         srcAddress = address(srcBuffer) + srcIndex;
         srcObj = null;
      } else {
         srcAddress = Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset(srcBuffer) + srcIndex;
         srcObj = array(srcBuffer);
      }
      //TODO employ guard against long waits to safepoint issues, 4K aliasing and page alignments memcpy
      UnsafeAccess.UNSAFE.copyMemory(srcObj, srcAddress, dstObj, dstAddress, length);
   }

   public static void copy(long srcAddress, Object srcObj, ByteBuffer dstBuffer, long dstIndex, int length) {
      final Object dstObj;
      final long dstAddress;
      if (dstBuffer.isDirect()) {
         dstAddress = address(dstBuffer) + dstIndex;
         dstObj = null;
      } else {
         dstAddress = Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset(dstBuffer) + dstIndex;
         dstObj = array(dstBuffer);
      }
      //TODO employ guard against long waits to safepoint issues, 4K aliasing and page alignments memcpy
      UnsafeAccess.UNSAFE.copyMemory(srcObj, srcAddress, dstObj, dstAddress, length);
   }

   public static void copy(ByteBuffer srcBuffer, int srcIndex, ByteBuffer dstBuffer, long dstIndex, int length) {
      final Object srcObj;
      final long srcAddress;
      if (srcBuffer.isDirect()) {
         srcAddress = address(srcBuffer) + srcIndex;
         srcObj = null;
      } else {
         srcAddress = Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset(srcBuffer) + srcIndex;
         srcObj = array(srcBuffer);
      }
      final Object dstObj;
      final long dstAddress;
      if (dstBuffer.isDirect()) {
         dstAddress = address(dstBuffer) + dstIndex;
         dstObj = null;
      } else {
         dstAddress = Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset(dstBuffer) + dstIndex;
         dstObj = array(dstBuffer);
      }
      //TODO employ guard against long waits to safepoint issues, 4K aliasing and page alignments memcpy
      UnsafeAccess.UNSAFE.copyMemory(srcObj, srcAddress, dstObj, dstAddress, length);
   }
}
