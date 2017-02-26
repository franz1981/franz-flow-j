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

import net.forked.franz.ringbuffer.utils.BytesUtils;

/**
 * Layout for the underlying ByteBuffer used by a {@link RingBuffer}.
 * The buffer consists of a ring of messages which is a power of 2 in size, followed by a trailer section containing state
 * information for the producers and consumers of the ring.
 */
final class RingBufferLayout {

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
    * Check that the buffer capacity is the correct size (a power of 2 + {@link RingBufferLayout#TRAILER_LENGTH}).
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