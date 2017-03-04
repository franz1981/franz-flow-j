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
import java.util.function.Supplier;

import net.forked.franz.ringbuffer.utils.BytesUtils;

final class RefFastFlowScRingBufferWrapper<T> implements RefRingBuffer<T> {

   private final FastFlowScRingBuffer ringBuffer;
   private final T[] refs;
   private final int refCapacity;
   private final int log2messageAlignment;

   @SuppressWarnings("unchecked")
   RefFastFlowScRingBufferWrapper(final FastFlowScRingBuffer ringBuffer, final Supplier<? extends T> refFactory) {
      this.ringBuffer = ringBuffer;
      if (!BytesUtils.isPowOf2(ringBuffer.messageAlignment())) {
         throw new IllegalArgumentException("the ring buffer used do not have a power of 2 's messageAlignment");
      }
      this.log2messageAlignment = Integer.numberOfTrailingZeros(ringBuffer.messageAlignment());
      this.refCapacity = ringBuffer.capacity() >> log2messageAlignment;
      this.refs = (T[]) new Object[refCapacity];
      for (int i = 0; i < refCapacity; i++) {
         refs[i] = refFactory.get();
      }

   }

   private static <T> T getRefAt(T[] refs, int offset, int log2messageAlignment) {
      final int refIndex = refIndex(offset, log2messageAlignment);
      final T ref = refs[refIndex];
      return ref;
   }

   private static int refIndex(final int msgContentIndex, final int log2MessageAlignment) {
      final int refIndex = (msgContentIndex - MessageLayout.HEADER_LENGTH) >> log2MessageAlignment;
      return refIndex;
   }

   @Override
   public int messageAlignment() {
      return this.ringBuffer.messageAlignment();
   }

   @Override
   public int headerSize() {
      return ringBuffer.headerSize();
   }

   @Override
   public int capacity() {
      return ringBuffer.capacity();
   }

   @Override
   public long write(int msgTypeId, ByteBuffer srcBuffer, int srcIndex, int length) {
      return ringBuffer.write(msgTypeId, srcBuffer, srcIndex, length);
   }

   @Override
   public <A> long write(int msgTypeId, int length, MessageTranslator<? super A> translator, A arg) {
      return ringBuffer.write(msgTypeId, length, translator, arg);
   }

   @Override
   public int read(MessageConsumer consumer) {
      return ringBuffer.read(consumer);
   }

   @Override
   public int read(MessageConsumer consumer, int messageCountLimit) {
      return ringBuffer.read(consumer, messageCountLimit);
   }

   @Override
   public int refCapacity() {
      return refCapacity;
   }

   @Override
   public <A1, A2> long write(int msgTypeId,
                              int length,
                              MessageRefTranslator<? super T, ? super A1, ? super A2> translator,
                              A1 arg1,
                              A2 arg2) {
      MessageLayout.checkMsgTypeId(msgTypeId);
      this.ringBuffer.checkMsgLength(length);
      final int msgLength = length + MessageLayout.HEADER_LENGTH;
      final int requiredMsgCapacity = (int) BytesUtils.align(msgLength, this.ringBuffer.messageAlignment());
      final long acquireResult = this.ringBuffer.writeAcquire(requiredMsgCapacity);
      if (acquireResult >= 0) {
         final long producerStartPosition = acquireResult;
         final int msgIndex = (int) (producerStartPosition & (this.ringBuffer.capacity() - 1));
         final int msgContentIndex = MessageLayout.msgContentOffset(msgIndex);
         final T ref = getRefAt(this.refs, msgContentIndex, this.log2messageAlignment);
         translator.translate(ref, msgTypeId, this.ringBuffer.buffer(), msgContentIndex, length, arg1, arg2);
         //WriteWrite
         this.ringBuffer.storeOrderedMsgHeader(msgIndex, MessageLayout.packHeaderWith(msgTypeId, msgLength));
         return producerStartPosition + requiredMsgCapacity;
      } else {
         return acquireResult;
      }
   }

   @Override
   public int read(MessageRefConsumer<? super T> consumer) {
      return read(consumer, Integer.MAX_VALUE);
   }

   @Override
   public int read(MessageRefConsumer<? super T> consumer, int messageCountLimit) {
      int msgRead = 0;
      final FastFlowScRingBuffer ringBuffer = this.ringBuffer;
      final ByteBuffer buffer = ringBuffer.buffer();
      final int capacity = ringBuffer.capacity();
      final int messageAlignment = ringBuffer.messageAlignment();
      final long consumerPosition = ringBuffer.loadConsumerPosition();
      final int consumerIndex = (int) consumerPosition & (capacity - 1);
      final int remainingBytes = capacity - consumerIndex;
      final T[] refs = this.refs;
      final int log2messageAlignment = this.log2messageAlignment;
      int bytesConsumed = 0;
      try {
         while ((bytesConsumed < remainingBytes) && (msgRead < messageCountLimit)) {
            final int msgIndex = consumerIndex + bytesConsumed;
            final long msgHeader = ringBuffer.loadVolatileMsgHeader(msgIndex);
            //LoadLoad + LoadStore
            final int msgLength = MessageLayout.length(msgHeader);
            if (msgLength <= 0) {
               break;
            }
            final int requiredMsgLength = (int) BytesUtils.align(msgLength, messageAlignment);
            bytesConsumed += requiredMsgLength;
            final int msgTypeId = MessageLayout.msgTypeId(msgHeader);
            if (msgTypeId != FastFlowScRingBuffer.PADDING_MSG_TYPE_ID) {
               msgRead++;
               final int msgContentLength = msgLength - MessageLayout.HEADER_LENGTH;
               final int msgContentIndex = msgIndex + MessageLayout.HEADER_LENGTH;
               final T ref = getRefAt(refs, msgContentIndex, log2messageAlignment);
               consumer.accept(ref, msgTypeId, buffer, msgContentIndex, msgContentLength);
            }
         }
      } finally {
         if (bytesConsumed != 0) {
            //zeros all the consumed bytes
            BytesUtils.zeros(buffer, consumerIndex, bytesConsumed);
            final long newConsumerPosition = consumerPosition + bytesConsumed;
            //StoreStore
            ringBuffer.storeOrderedConsumerPosition(newConsumerPosition);
         }
      }
      return msgRead;
   }

   @Override
   public int maxMessageLength() {
      return this.ringBuffer.maxMessageLength();
   }

   @Override
   public ByteBuffer buffer() {
      return this.ringBuffer.buffer();
   }

   @Override
   public long producerPosition() {
      return this.ringBuffer.producerPosition();
   }

   @Override
   public long consumerPosition() {
      return this.ringBuffer.consumerPosition();
   }

   @Override
   public int size() {
      return this.ringBuffer.size();
   }
}
