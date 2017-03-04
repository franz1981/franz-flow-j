/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.forked.franz.ringbuffer;

import java.nio.ByteBuffer;

import net.forked.franz.ringbuffer.utils.BytesUtils;
import net.forked.franz.ringbuffer.utils.UnsafeAccess;
import sun.misc.Unsafe;

abstract class LamportProducerFastFlowScRingBuffer extends FastFlowScRingBuffer {

   private final int capacity;
   private final int maxMsgLength;
   private final int producerPositionIndex;
   private final int consumerCachePositionIndex;
   private final int consumerPositionIndex;
   private final ByteBuffer buffer;
   private final Object bufferObj;
   private final long bufferAddress;
   private final int messageAlignment;

   protected LamportProducerFastFlowScRingBuffer(final ByteBuffer buffer, int messageAlignment) {
      if (!BytesUtils.isAligned(messageAlignment, MessageLayout.DEFAULT_ALIGNMENT)) {
         throw new IllegalStateException("a custom messageAlignment must be aligned to the default message alignment!");
      }
      RingBufferLayout.checkCapacity(buffer.capacity());
      this.buffer = buffer;
      this.bufferAddress = buffer.isDirect() ? BytesUtils.address(buffer) : Unsafe.ARRAY_BYTE_BASE_OFFSET + BytesUtils.arrayOffset(buffer);
      this.bufferObj = buffer.isDirect() ? null : BytesUtils.array(buffer);
      if (!BytesUtils.isAligned(bufferAddress, Long.BYTES)) {
         throw new IllegalStateException("buffer must be aligned to " + Long.BYTES + " bytes!");
      }
      capacity = buffer.capacity() - RingBufferLayout.TRAILER_LENGTH;
      if (!BytesUtils.isAligned(capacity, messageAlignment)) {
         throw new IllegalStateException("the capacity must be aligned to messageAlignment!");
      }
      //it is allowed to have only one big message
      maxMsgLength = capacity - MessageLayout.HEADER_LENGTH;
      producerPositionIndex = capacity + RingBufferLayout.PRODUCER_POSITION_OFFSET;
      consumerCachePositionIndex = capacity + RingBufferLayout.CONSUMER_CACHE_POSITION_OFFSET;
      consumerPositionIndex = capacity + RingBufferLayout.CONSUMER_POSITION_OFFSET;
      this.messageAlignment = messageAlignment;
   }

   @Override
   public final ByteBuffer buffer() {
      return buffer;
   }

   @Override
   protected final Object bufferObj() {
      return bufferObj;
   }

   @Override
   protected final long bufferAddress() {
      return bufferAddress;
   }

   @Override
   public final int capacity() {
      return capacity;
   }

   protected final long isFull(final long producerPosition, final int requiredCapacity) {
      final long consumerPosition = loadVolatileConsumerPosition();
      //LoadLoad + LoadStore
      if (requiredCapacity > (capacity - (int) (producerPosition - consumerPosition))) {
         return FULL;
      } else {
         storeConsumerCachePosition(consumerPosition);
         return consumerPosition;
      }
   }

   private long acquireFromStartOfBuffer(final int requiredCapacity, final int mask) {
      final long consumerPosition = loadVolatileConsumerPosition();
      //LoadLoad + LoadStore
      final int consumerIndex = (int) consumerPosition & mask;
      if (requiredCapacity > consumerIndex) {
         //the consumerIndex isn't moved enough
         return FULL;
      } else {
         storeConsumerCachePosition(consumerPosition);
         return consumerPosition;
      }
   }

   protected final long needPad(final long consumerPosition, final int requiredCapacity, final int mask) {
      final int consumerIndex = (int) consumerPosition & mask;
      if (requiredCapacity > consumerIndex) {
         //there isn't enough space between the start of the buffer and the consumerIndex
         return acquireFromStartOfBuffer(requiredCapacity, mask);
      }
      return consumerPosition;
   }

   protected final boolean casProducerPosition(long expectedValue, long value) {
      return UnsafeAccess.UNSAFE.compareAndSwapLong(bufferObj, bufferAddress + producerPositionIndex, expectedValue, value);
   }

   @Override
   public final int messageAlignment() {
      return messageAlignment;
   }

   @Override
   public final int maxMessageLength() {
      return maxMsgLength;
   }

   @Override
   public final long consumerPosition() {
      return loadVolatileConsumerPosition();
   }

   @Override
   public final long producerPosition() {
      return loadVolatileProducerPosition();
   }

   protected final long loadConsumerPosition() {
      return UnsafeAccess.UNSAFE.getLong(bufferObj, bufferAddress + consumerPositionIndex);
   }

   protected final void storeOrderedConsumerPosition(long value) {
      UnsafeAccess.UNSAFE.putOrderedLong(bufferObj, bufferAddress + consumerPositionIndex, value);
   }

   protected final long loadVolatileProducerPosition() {
      return UnsafeAccess.UNSAFE.getLongVolatile(bufferObj, bufferAddress + producerPositionIndex);
   }

   protected final long loadProducerPosition() {
      return UnsafeAccess.UNSAFE.getLong(bufferObj, bufferAddress + producerPositionIndex);
   }

   protected final void storeOrderedProducerPosition(long value) {
      UnsafeAccess.UNSAFE.putOrderedLong(bufferObj, bufferAddress + producerPositionIndex, value);
   }

   private final long loadVolatileConsumerPosition() {
      return UnsafeAccess.UNSAFE.getLongVolatile(bufferObj, bufferAddress + consumerPositionIndex);
   }

   protected final long loadConsumerCachePosition() {
      return UnsafeAccess.UNSAFE.getLong(bufferObj, bufferAddress + consumerCachePositionIndex);
   }

   private final void storeConsumerCachePosition(long value) {
      UnsafeAccess.UNSAFE.putLong(bufferObj, bufferAddress + consumerCachePositionIndex, value);
   }

   /**
    * Layout for the underlying ByteBuffer used by a {@link RingBuffer}.
    * The buffer consists of a ring of messages which is a power of 2 in size, followed by a trailer section containing state
    * information for the producers and consumers of the ring.
    */
   static final class RingBufferLayout {

      /**
       * Offset within the trailer for where the producer value is stored.
       */
      public static final int PRODUCER_POSITION_OFFSET;
      /**
       * Offset within the trailer for where the consumer cache value is stored.
       */
      public static final int CONSUMER_CACHE_POSITION_OFFSET;
      /**
       * Offset within the trailer for where the head value is stored.
       */
      public static final int CONSUMER_POSITION_OFFSET;
      /**
       * Total length of the trailer in bytes.
       */
      public static final int TRAILER_LENGTH;

      static {
         int offset = 0;
         offset += (BytesUtils.CACHE_LINE_LENGTH * 2);
         PRODUCER_POSITION_OFFSET = offset;

         offset += (BytesUtils.CACHE_LINE_LENGTH * 2);
         CONSUMER_CACHE_POSITION_OFFSET = offset;

         offset += (BytesUtils.CACHE_LINE_LENGTH * 2);
         CONSUMER_POSITION_OFFSET = offset;

         offset += (BytesUtils.CACHE_LINE_LENGTH * 2);
         TRAILER_LENGTH = offset;
      }

      private RingBufferLayout() {
      }

      /**
       * Check that the buffer capacity is the correct size (a power of 2 + {@link net.forked.franz.ringbuffer.RingBufferLayout#TRAILER_LENGTH}).
       *
       * @param capacity to be checked.
       * @throws IllegalStateException if the buffer capacity is incorrect.
       */
      public static void checkCapacity(final int capacity) {
         if (!BytesUtils.isPowOf2(capacity - TRAILER_LENGTH)) {
            final String msg = "Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=" + capacity;
            throw new IllegalStateException(msg);
         }
      }

      public static int capacity(final int requestedCapacity) {
         final int ringBufferCapacity = BytesUtils.nextPowOf2(requestedCapacity) + TRAILER_LENGTH;
         if (ringBufferCapacity < 0) {
            throw new IllegalArgumentException("requestedCapacity is too big!");
         }
         return ringBufferCapacity;
      }
   }
}
