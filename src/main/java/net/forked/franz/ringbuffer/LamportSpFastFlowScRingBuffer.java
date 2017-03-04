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

/**
 * A ring-buffer that supports the exchange of messages from a single producers to a single consumer.
 */

final class LamportSpFastFlowScRingBuffer extends LamportProducerFastFlowScRingBuffer {

   LamportSpFastFlowScRingBuffer(final ByteBuffer buffer, int messageAlignment) {
      super(buffer, messageAlignment);
   }

   @Override
   protected final long writeAcquire(final int requiredCapacity) {
      final int capacity = this.capacity();
      final int mask = capacity - 1;
      long consumerPosition = loadConsumerCachePosition();
      final long producerPosition = loadProducerPosition();
      int producerIndex;
      int padding;
      final int availableCapacity = capacity - (int) (producerPosition - consumerPosition);
      if (requiredCapacity > availableCapacity) {
         final long position = isFull(producerPosition, requiredCapacity);
         if (position == FULL) {
            return FULL;
         } else {
            consumerPosition = position;
         }
      }
      padding = 0;
      producerIndex = (int) producerPosition & mask;
      final int bytesUntilEndOfBuffer = capacity - producerIndex;
      if (requiredCapacity > bytesUntilEndOfBuffer) {
         //need pad!
         final long position = needPad(consumerPosition, requiredCapacity, mask);
         if (position == FULL) {
            return FULL;
         }
         padding = bytesUntilEndOfBuffer;
      }
      //StoreStore
      storeOrderedProducerPosition(producerPosition + requiredCapacity + padding);
      if (padding != 0) {
         //StoreStore
         storeOrderedMsgHeader(producerIndex, MessageLayout.packHeaderWith(PADDING_MSG_TYPE_ID, padding));
      }
      return producerPosition + padding;
   }

}