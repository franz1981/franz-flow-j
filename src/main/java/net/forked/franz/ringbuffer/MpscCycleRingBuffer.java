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

final class MpscCycleRingBuffer implements RingBuffer {

   /**
    * RingBuffer could contain a pad to prevent fragmentation of long messages.
    */
   public static final int PADDING_MSG_TYPE_ID = -1;
   /**
    * RingBuffer has insufficient capacity to write a message.
    */
   private final int capacity;
   private final int mask;
   private final int maxMsgLength;
   private final int maxGain;
   private final int maskCycleLength;
   private final int cycleLength;
   private final int maskCycles;
   private final int cycles;
   private final int activeCycleIndex;
   private final int consumerCachePositionIndex;
   private final int consumerPositionIndex;
   private final int[] producerCycleClaimIndex;
   private final ByteBuffer buffer;
   private final Object bufferObj;
   private final long bufferAddress;
   private final int messageAlignment;

   public MpscCycleRingBuffer(final ByteBuffer buffer, final int messageAlignment, final int cycles) {
      if (cycles < 2) {
         throw new IllegalStateException("cycle must be > 1!");
      }
      if (!BytesUtils.isAligned(messageAlignment, MessageLayout.DEFAULT_ALIGNMENT)) {
         throw new IllegalStateException("a custom messageAlignment must be aligned to the default message alignment!");
      }
      this.buffer = buffer;
      this.bufferAddress = buffer.isDirect() ? BytesUtils.address(buffer) : Unsafe.ARRAY_BYTE_BASE_OFFSET + BytesUtils.arrayOffset(buffer);
      this.messageAlignment = messageAlignment;
      this.bufferObj = buffer.isDirect() ? null : BytesUtils.array(buffer);
      if (!BytesUtils.isAligned(bufferAddress, Long.BYTES)) {
         throw new IllegalStateException("buffer must be aligned to " + Long.BYTES + " bytes!");
      }
      this.cycles = BytesUtils.nextPowOf2(cycles);
      this.maskCycles = this.cycles - 1;
      this.capacity = buffer.capacity() - RingBufferLayout.trailerLength(this.cycles);
      if (!BytesUtils.isPowOf2(this.capacity)) {
         throw new IllegalStateException("capacity must be a power of 2!");
      }
      this.mask = this.capacity - 1;
      if (!BytesUtils.isAligned(this.capacity, this.messageAlignment)) {
         throw new IllegalStateException("the capacity must be aligned to messageAlignment!");
      }
      this.cycleLength = this.capacity / this.cycles;
      //cycleLength is by definition a power of 2!!!
      this.maskCycleLength = this.cycleLength - 1;
      //No producers can claim positions in the same cycle index of the consumer.
      //if the back-pressure fails to stop a producer, it won't succeed to claim more than the cycle limit:
      //if the claim reaches the cycle limit it will cause a cycle rotation (that refreshes the current cycle offset for any producers)
      // and the new positions claims will fail against the max gain limit.
      this.maxGain = this.cycleLength * (this.cycles - 1);
      this.activeCycleIndex = this.capacity + RingBufferLayout.ACTIVE_CYCLE_INDEX_OFFSET;
      this.consumerCachePositionIndex = this.capacity + RingBufferLayout.CONSUMER_CACHE_POSITION_OFFSET;
      this.consumerPositionIndex = this.capacity + RingBufferLayout.CONSUMER_POSITION_OFFSET;
      this.producerCycleClaimIndex = new int[this.cycles];
      for (int i = 0; i < this.cycles; i++) {
         this.producerCycleClaimIndex[i] = this.capacity + RingBufferLayout.PRODUCERS_CYCLE_CLAIM_OFFSET + (i * Long.BYTES);
      }
      //it is allowed to have only one big message
      this.maxMsgLength = this.cycleLength - MessageLayout.HEADER_LENGTH;
   }

   @Override
   public int messageAlignment() {
      return this.messageAlignment;
   }

   @Override
   public int headerSize() {
      return MessageLayout.HEADER_LENGTH;
   }

   @Override
   public int capacity() {
      return this.capacity;
   }

   protected final void checkMsgLength(final int length) {
      if (length > maxMsgLength) {
         throw new IllegalArgumentException(String.format("message content exceeds maxMessageLength of %d, length=%d", maxMsgLength, length));
      }
   }

   private final long writeAcquire(final int requiredCapacity) {
      final int maxGain = this.maxGain;
      final int cycleLength = this.cycleLength;

      final int activeCycleIndex = loadVolatileActiveCycleIndex();
      final long producerCycleClaim = loadVolatileProducerCycleClaim(activeCycleIndex);
      final long producerPosition = RingBufferLayout.producerPosition(producerCycleClaim, cycleLength);
      final long claimLimit = loadConsumerCachePosition() + maxGain;
      final long expectedNextProducerPosition = producerPosition + requiredCapacity;
      if (expectedNextProducerPosition >= claimLimit) {
         if (isBackpressured(expectedNextProducerPosition, maxGain)) {
            return -1;
         }
      }
      //try to claim on the current active cycle
      final long startingProducerCycleClaim = getAndAddProducerCycleClaim(activeCycleIndex, requiredCapacity);
      final long cyclePosition = RingBufferLayout.cyclePosition(startingProducerCycleClaim);
      final long claimedCyclePosition = cyclePosition + requiredCapacity;
      //is an overclaim?
      if (claimedCyclePosition > cycleLength) {
         //it is the first attempt to claim more than the remaining space in the cycle?
         if (cyclePosition <= cycleLength) {
            padAndRotateCycle(activeCycleIndex, RingBufferLayout.cycleId(startingProducerCycleClaim), cyclePosition, cycleLength);
         }
         return -1;
      } else {
         //all goes straight, return the absolute claimed position
         return RingBufferLayout.producerPosition(startingProducerCycleClaim, cycleLength);
      }
   }

   private final void padAndRotateCycle(int activeCycleIndex, long cycleId, long cyclePosition, int cycleLength) {
      //is it needed a pad?
      if (cyclePosition < cycleLength) {
         final int padLength = (int) (cycleLength - cyclePosition);
         final int padOffset = RingBufferLayout.offset(activeCycleIndex, (int) cyclePosition, cycleLength);
         //ordered is not really required..is only to ensure atomicity :( on C11 a relaxed store is sufficient
         storeOrderedMsgHeader(padOffset, MessageLayout.packHeaderWith(PADDING_MSG_TYPE_ID, padLength));
      }
      //rotate cycle
      final int nextActiveCycleIndex = (activeCycleIndex + 1) & this.maskCycles;
      final long nextCycleId = cycleId + 1;
      final long nextProducerCycleClaim = nextCycleId << 32;
      //prepare the next producer claim
      //uses a plain store because its value will be released by the active cycle store
      storeProducerCycleClaim(nextActiveCycleIndex, nextProducerCycleClaim);
      storeOrderedActiveCycleIndex(nextActiveCycleIndex);
   }

   protected final void storeOrderedMsgHeader(int messageIndex, long header) {
      UnsafeAccess.UNSAFE.putOrderedLong(bufferObj, bufferAddress + messageIndex, header);
   }

   private final boolean isBackpressured(final long producerPosition, final int maxGain) {
      //load volatile is necessary only for atomicity
      final long consumerPosition = loadVolatileConsumerPosition();
      final long claimLimit = consumerPosition + maxGain;
      if (producerPosition < claimLimit) {
         //update the cached consumer position for the other producers
         storeConsumerCachePosition(consumerPosition);
         return false;
      } else {
         return true;
      }
   }

   @Override
   public long write(int msgTypeId, ByteBuffer srcBuffer, int srcIndex, int length) {
      MessageLayout.checkMsgTypeId(msgTypeId);
      checkMsgLength(length);
      final int msgLength = length + MessageLayout.HEADER_LENGTH;
      final int requiredMsgCapacity = (int) BytesUtils.align(msgLength, this.messageAlignment);
      final long acquireResult = writeAcquire(requiredMsgCapacity);
      if (acquireResult >= 0) {
         final long producerStartPosition = acquireResult;
         final int msgIndex = (int) (producerStartPosition & mask);
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

   @Override
   public <A> long write(int msgTypeId, int length, MessageTranslator<? super A> translator, A arg) {
      MessageLayout.checkMsgTypeId(msgTypeId);
      checkMsgLength(length);
      final int msgLength = length + MessageLayout.HEADER_LENGTH;
      final int requiredMsgCapacity = (int) BytesUtils.align(msgLength, this.messageAlignment);
      final long acquireResult = writeAcquire(requiredMsgCapacity);
      if (acquireResult >= 0) {
         final long producerStartPosition = acquireResult;
         final int msgIndex = (int) (producerStartPosition & mask);
         final int msgContentIndex = MessageLayout.msgContentOffset(msgIndex);
         translator.translate(msgTypeId, buffer, msgContentIndex, length, arg);
         //WriteWrite
         storeOrderedMsgHeader(msgIndex, MessageLayout.packHeaderWith(msgTypeId, msgLength));
         return producerStartPosition + requiredMsgCapacity;
      } else {
         return acquireResult;
      }
   }

   @Override
   public int read(MessageConsumer consumer) {
      return read(consumer, Integer.MAX_VALUE);
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
      final int consumerIndex = (int) consumerPosition & this.mask;
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
            //StoreStore + LoadStore
            storeOrderedConsumerPosition(newConsumerPosition);
         }
      }
      return msgRead;
   }

   private final int loadVolatileActiveCycleIndex() {
      return UnsafeAccess.UNSAFE.getIntVolatile(bufferObj, bufferAddress + activeCycleIndex);
   }

   private final void storeOrderedActiveCycleIndex(int cycleIndex) {
      UnsafeAccess.UNSAFE.putOrderedInt(bufferObj, bufferAddress + activeCycleIndex, cycleIndex);
   }

   private final long loadVolatileProducerCycleClaim(final int cycleIndex) {
      return UnsafeAccess.UNSAFE.getLongVolatile(bufferObj, bufferAddress + producerCycleClaimIndex[cycleIndex]);
   }

   private final long getAndAddProducerCycleClaim(final int cycleIndex, long delta) {
      return UnsafeAccess.UNSAFE.getAndAddLong(bufferObj, bufferAddress + producerCycleClaimIndex[cycleIndex], delta);
   }

   private final void storeProducerCycleClaim(final int cycleIndex, long value) {
      UnsafeAccess.UNSAFE.putLong(bufferObj, bufferAddress + producerCycleClaimIndex[cycleIndex], value);
   }

   private final void storeConsumerCachePosition(long value) {
      UnsafeAccess.UNSAFE.putLong(bufferObj, bufferAddress + consumerCachePositionIndex, value);
   }

   private final long loadConsumerCachePosition() {
      return UnsafeAccess.UNSAFE.getLong(bufferObj, bufferAddress + consumerCachePositionIndex);
   }

   private final long loadVolatileMsgHeader(int msgIndex) {
      return UnsafeAccess.UNSAFE.getLongVolatile(bufferObj, bufferAddress + msgIndex);
   }

   private final long loadVolatileConsumerPosition() {
      return UnsafeAccess.UNSAFE.getLongVolatile(bufferObj, bufferAddress + consumerPositionIndex);
   }

   private final long loadConsumerPosition() {
      return UnsafeAccess.UNSAFE.getLong(bufferObj, bufferAddress + consumerPositionIndex);
   }

   private final void storeOrderedConsumerPosition(long value) {
      UnsafeAccess.UNSAFE.putOrderedLong(bufferObj, bufferAddress + consumerPositionIndex, value);
   }

   @Override
   public int maxMessageLength() {
      return this.maxMsgLength;
   }

   @Override
   public ByteBuffer buffer() {
      return this.buffer;
   }

   @Override
   public long producerPosition() {
      final int activeCycleIndex = loadVolatileActiveCycleIndex();
      final long producerCycleClaim = loadVolatileProducerCycleClaim(activeCycleIndex);
      return RingBufferLayout.normalizedProducerPosition(producerCycleClaim, this.cycleLength);
   }

   @Override
   public long consumerPosition() {
      return loadVolatileConsumerPosition();
   }

   @Override
   public int size() {
      return (int) (producerPosition() - consumerPosition());
   }

   static final class RingBufferLayout {

      public static final int ACTIVE_CYCLE_INDEX_OFFSET;
      public static final int CONSUMER_CACHE_POSITION_OFFSET;
      public static final int CONSUMER_POSITION_OFFSET;
      public static final int PRODUCERS_CYCLE_CLAIM_OFFSET;

      static {
         int offset = 0;
         offset += (BytesUtils.CACHE_LINE_LENGTH * 2);
         ACTIVE_CYCLE_INDEX_OFFSET = offset;

         offset += (BytesUtils.CACHE_LINE_LENGTH * 2);
         CONSUMER_CACHE_POSITION_OFFSET = offset;

         offset += (BytesUtils.CACHE_LINE_LENGTH * 2);
         CONSUMER_POSITION_OFFSET = offset;

         offset += (BytesUtils.CACHE_LINE_LENGTH * 2);
         PRODUCERS_CYCLE_CLAIM_OFFSET = offset;
      }

      private RingBufferLayout() {
      }

      public static int offset(int cycleIndex, int cyclePosition, int cycleLength) {
         return (cycleIndex * cycleLength) + cyclePosition;
      }

      public static long producerPosition(long producerCycleClaim, int cycleLength) {
         final long cyclePosition = cyclePosition(producerCycleClaim);
         final long cycleId = cycleId(producerCycleClaim);
         final long producerPosition = (cycleId * cycleLength) + cyclePosition;
         return producerPosition;
      }

      public static long normalizedProducerPosition(long producerCycleClaim, int cycleLength) {
         long cyclePosition = cyclePosition(producerCycleClaim);
         //normalization
         cyclePosition = Math.min(cyclePosition, cycleLength);
         final long cycleId = cycleId(producerCycleClaim);
         final long producerPosition = (cycleId * cycleLength) + cyclePosition;
         return producerPosition;
      }

      public static long cyclePosition(long producerCycleClaim) {
         return producerCycleClaim & 0xFFFFFFFFL;
      }

      public static long cycleId(long producerCycleClaim) {
         return producerCycleClaim >> 32;
      }

      public static int trailerLength(final int cycles) {
         final int trailerLength = PRODUCERS_CYCLE_CLAIM_OFFSET + (int) BytesUtils.align(BytesUtils.nextPowOf2(cycles) * Long.BYTES, BytesUtils.CACHE_LINE_LENGTH * 2);
         return trailerLength;
      }
   }
}
