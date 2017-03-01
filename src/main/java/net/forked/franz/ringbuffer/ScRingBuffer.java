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

/**
 * A ring-buffer that supports the exchange of messages from many producers to a single consumer.
 */

abstract class ScRingBuffer implements RingBuffer {

   /**
    * RingBuffer could contain a pad to prevent fragmentation of long messages.
    */
   public static final int PADDING_MSG_TYPE_ID = -1;
   /**
    * RingBuffer has insufficient capacity to write a message.
    */
   private final int capacity;
   private final int maxMsgLength;
   private final int producerPositionIndex;
   private final int consumerCachePositionIndex;
   private final int consumerPositionIndex;
   private final ByteBuffer buffer;
   private final Object bufferObj;
   private final long bufferAddress;
   private final int messageAlignment;

   /**
    * Construct a new {@link RingBuffer} based on an underlying {@link ByteBuffer}.
    * The underlying buffer must a power of 2 in size plus sufficient space
    * for the {@link RingBufferLayout#TRAILER_LENGTH}.
    *
    * @param buffer via which events will be exchanged.
    * @throws IllegalStateException if the buffer capacity is not a power of 2
    *                               plus {@link RingBufferLayout#TRAILER_LENGTH} in capacity.
    */
   protected ScRingBuffer(final ByteBuffer buffer, int messageAlignment) {
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

   /**
    * {@inheritDoc}
    */
   @Override
   public final int messageAlignment() {
      return messageAlignment;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final int headerSize() {
      return MessageLayout.HEADER_LENGTH;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final int capacity() {
      return capacity;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final long write(final int msgTypeId, final ByteBuffer srcBuffer, final int srcIndex, final int length) {
      MessageLayout.checkMsgTypeId(msgTypeId);
      checkMsgLength(length);
      final int msgLength = length + MessageLayout.HEADER_LENGTH;
      final int requiredMsgCapacity = (int) BytesUtils.align(msgLength, this.messageAlignment);
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
      final int requiredMsgCapacity = (int) BytesUtils.align(msgLength, this.messageAlignment);
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
      final ByteBuffer buffer = this.buffer;
      final int capacity = this.capacity;
      final int messageAlignment = this.messageAlignment;
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
      UnsafeAccess.UNSAFE.putOrderedLong(bufferObj, bufferAddress + messageIndex, header);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final int read(final MessageConsumer consumer) {
      return read(consumer, Integer.MAX_VALUE);
   }

   protected final long loadVolatileMsgHeader(int msgIndex) {
      return UnsafeAccess.UNSAFE.getLongVolatile(bufferObj, bufferAddress + msgIndex);
   }

   protected final long loadConsumerPosition() {
      return UnsafeAccess.UNSAFE.getLong(bufferObj, bufferAddress + consumerPositionIndex);
   }

   protected final void storeOrderedConsumerPosition(long value) {
      UnsafeAccess.UNSAFE.putOrderedLong(bufferObj, bufferAddress + consumerPositionIndex, value);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final int maxMessageLength() {
      return maxMsgLength;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public ByteBuffer buffer() {
      return buffer;
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

   /**
    * {@inheritDoc}
    */
   @Override
   public final long producerPosition() {

      return loadVolatileProducerPosition();
   }

   protected final long loadVolatileConsumerPosition() {
      return UnsafeAccess.UNSAFE.getLongVolatile(bufferObj, bufferAddress + consumerPositionIndex);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final long consumerPosition() {
      return loadVolatileConsumerPosition();
   }

   protected final long loadConsumerCachePosition() {
      return UnsafeAccess.UNSAFE.getLong(bufferObj, bufferAddress + consumerCachePositionIndex);
   }

   private void storeConsumerCachePosition(long value) {
      UnsafeAccess.UNSAFE.putLong(bufferObj, bufferAddress + consumerCachePositionIndex, value);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public final int size() {
      long previousConsumerPosition;
      long producerPosition;
      long consumerPosition = loadVolatileConsumerPosition();

      do {
         previousConsumerPosition = consumerPosition;
         producerPosition = loadVolatileProducerPosition();
         consumerPosition = loadVolatileConsumerPosition();
      } while (consumerPosition != previousConsumerPosition);
      final int size = (int) (producerPosition - consumerPosition);
      return size;
   }

   protected final void checkMsgLength(final int length) {
      if (length > maxMsgLength) {
         throw new IllegalArgumentException(String.format("message content exceeds maxMessageLength of %d, length=%d", maxMsgLength, length));
      }
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

}