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

/**
 * A ring-buffer that supports the exchange of messages from many producers to a single consumer.
 */

abstract class FastFlowScRingBuffer implements RingBuffer {

   /**
    * RingBuffer could contain a pad to prevent fragmentation of long messages.
    */
   public static final int PADDING_MSG_TYPE_ID = -1;

   /**
    * {@inheritDoc}
    */
   @Override
   public final int headerSize() {
      return MessageLayout.HEADER_LENGTH;
   }

   protected abstract long bufferAddress();

   protected abstract Object bufferObj();

   /**
    * {@inheritDoc}
    */
   @Override
   public final long write(final int msgTypeId, final ByteBuffer srcBuffer, final int srcIndex, final int length) {
      MessageLayout.checkMsgTypeId(msgTypeId);
      checkMsgLength(length);
      final int msgLength = length + MessageLayout.HEADER_LENGTH;
      final int messageAlignment = messageAlignment();
      final int capacity = capacity();
      final long bufferAddress = bufferAddress();
      final Object bufferObj = bufferObj();
      final int requiredMsgCapacity = (int) BytesUtils.align(msgLength, messageAlignment);
      final long acquireResult = writeAcquire(requiredMsgCapacity);
      if (acquireResult >= 0) {
         final long producerStartPosition = acquireResult;
         final int msgIndex = (int) (producerStartPosition & (capacity - 1));
         final int msgContentIndex = MessageLayout.msgContentOffset(msgIndex);
         final long msgContentAddress = bufferAddress + msgContentIndex;
         BytesUtils.copy(srcBuffer, srcIndex, bufferObj, msgContentAddress, length);
         //WriteWrite
         storeOrderedMsgHeader(msgIndex, MessageLayout.packHeaderWith(msgTypeId, msgLength));
         return producerStartPosition + requiredMsgCapacity;
      } else {
         return acquireResult;
      }
   }

   protected abstract long writeAcquire(final int requiredCapacity);

   /**
    * {@inheritDoc}
    */
   @Override
   public final <A> long write(final int msgTypeId,
                               final int length,
                               final MessageTranslator<? super A> translator,
                               A arg) {
      MessageLayout.checkMsgTypeId(msgTypeId);
      checkMsgLength(length);
      final int msgLength = length + MessageLayout.HEADER_LENGTH;
      final int messageAlignment = messageAlignment();
      final int capacity = capacity();
      final ByteBuffer buffer = buffer();
      final int requiredMsgCapacity = (int) BytesUtils.align(msgLength, messageAlignment);
      final long acquireResult = writeAcquire(requiredMsgCapacity);
      if (acquireResult >= 0) {
         final long producerStartPosition = acquireResult;
         final int msgIndex = (int) (producerStartPosition & (capacity - 1));
         final int msgContentIndex = MessageLayout.msgContentOffset(msgIndex);
         translator.translate(msgTypeId, buffer, msgContentIndex, length, arg);
         //WriteWrite
         storeOrderedMsgHeader(msgIndex, MessageLayout.packHeaderWith(msgTypeId, msgLength));
         return producerStartPosition + requiredMsgCapacity;
      } else {
         return acquireResult;
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final int read(final MessageConsumer consumer, final int count) {
      int msgRead = 0;
      final ByteBuffer buffer = buffer();
      final int capacity = capacity();
      final int messageAlignment = messageAlignment();
      final long consumerPosition = loadConsumerPosition();
      final int consumerIndex = (int) consumerPosition & (capacity - 1);
      final int remainingBytes = capacity - consumerIndex;
      int bytesConsumed = 0;
      try {
         while ((bytesConsumed < remainingBytes) && (msgRead < count)) {
            final int msgIndex = consumerIndex + bytesConsumed;
            final long msgHeader = loadVolatileMsgHeader(msgIndex);
            //LoadLoad + LoadStore
            final int msgLength = MessageLayout.length(msgHeader);
            if (msgLength <= 0) {
               break;
            }
            final int requiredMsgLength = (int) BytesUtils.align(msgLength, messageAlignment);
            bytesConsumed += requiredMsgLength;
            final int msgTypeId = MessageLayout.msgTypeId(msgHeader);
            if (msgTypeId != PADDING_MSG_TYPE_ID) {
               msgRead++;
               final int msgContentLength = msgLength - MessageLayout.HEADER_LENGTH;
               final int msgContentIndex = msgIndex + MessageLayout.HEADER_LENGTH;
               consumer.accept(msgTypeId, buffer, msgContentIndex, msgContentLength);
            }
         }
      } finally {
         if (bytesConsumed != 0) {
            //zeros all the consumed bytes
            BytesUtils.zeros(buffer, consumerIndex, bytesConsumed);
            final long newConsumerPosition = consumerPosition + bytesConsumed;
            //StoreStore
            storeOrderedConsumerPosition(newConsumerPosition);
         }
      }

      return msgRead;
   }

   protected final void storeOrderedMsgHeader(int messageIndex, long header) {
      UnsafeAccess.UNSAFE.putOrderedLong(bufferObj(), bufferAddress() + messageIndex, header);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final int read(final MessageConsumer consumer) {
      return read(consumer, Integer.MAX_VALUE);
   }

   protected final long loadVolatileMsgHeader(int msgIndex) {
      return UnsafeAccess.UNSAFE.getLongVolatile(bufferObj(), bufferAddress() + msgIndex);
   }

   protected abstract long loadConsumerPosition();

   protected abstract void storeOrderedConsumerPosition(long value);

   /**
    * {@inheritDoc}
    */
   @Override
   public abstract long producerPosition();

   /**
    * {@inheritDoc}
    */
   @Override
   public abstract long consumerPosition();

   /**
    * {@inheritDoc}
    */
   @Override
   public final int size() {
      long previousConsumerPosition;
      long producerPosition;
      long consumerPosition = consumerPosition();

      do {
         previousConsumerPosition = consumerPosition;
         producerPosition = producerPosition();
         consumerPosition = consumerPosition();
      } while (consumerPosition != previousConsumerPosition);
      final int size = (int) (producerPosition - consumerPosition);
      return size;
   }

   protected final void checkMsgLength(final int length) {
      final int maxMessageLength = maxMessageLength();
      if (length > maxMessageLength) {
         throw new IllegalArgumentException(String.format("message content exceeds maxMessageLength of %d, length=%d", maxMessageLength, length));
      }
   }

}