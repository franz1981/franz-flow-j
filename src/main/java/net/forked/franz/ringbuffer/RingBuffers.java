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

public final class RingBuffers {

   private static final int DEFAULT_CYCLES = 2;

   private RingBuffers() {

   }

   public static int capacity(RingBufferType type, int bytes) {
      switch (type) {

         case SingleProducerSingleConsumer:
            return LamportSpFastFlowScRingBuffer.RingBufferLayout.capacity(bytes);
         case MultiProducerSingleConsumer:
            return LamportSpFastFlowScRingBuffer.RingBufferLayout.capacity(bytes);
         case RelaxedMultiProducerSingleConsumer:
            return RelaxedMpFastFlowScRingBuffer.RingBufferLayout.capacity(bytes, DEFAULT_CYCLES);
      }
      throw new AssertionError("case not exists!");
   }

   public static int relaxedCapacity(int bytes, int cycles) {
      return RelaxedMpFastFlowScRingBuffer.RingBufferLayout.capacity(bytes, cycles);
   }

   public static <T> RefRingBuffer<T> withRef(RingBufferType type, ByteBuffer bytes, Supplier<? extends T> refFactory) {
      final FastFlowScRingBuffer ringBuffer = with(type, bytes, MessageLayout.DEFAULT_ALIGNMENT);
      return new RefFastFlowScRingBufferWrapper<>(ringBuffer, refFactory);
   }

   public static <T> RefRingBuffer<T> withRef(RingBufferType type,
                                              ByteBuffer bytes,
                                              Supplier<? extends T> refFactory,
                                              int averageMessageLength) {
      final int messageAlignment = messageAlignment(averageMessageLength);
      final FastFlowScRingBuffer ringBuffer = with(type, bytes, messageAlignment);
      return new RefFastFlowScRingBufferWrapper<>(ringBuffer, refFactory);
   }

   public static RingBuffer relaxed(ByteBuffer bytes, int cycles) {
      final int messageAlignment = MessageLayout.DEFAULT_ALIGNMENT;
      return new RelaxedMpFastFlowScRingBuffer(bytes, messageAlignment, cycles);
   }

   public static RingBuffer with(RingBufferType type, ByteBuffer bytes) {
      final int messageAlignment = MessageLayout.DEFAULT_ALIGNMENT;
      return with(type, bytes, messageAlignment);
   }

   private static FastFlowScRingBuffer with(RingBufferType type, ByteBuffer bytes, int messageAlignment) {
      final FastFlowScRingBuffer ringBuffer;
      switch (type) {

         case SingleProducerSingleConsumer:
            ringBuffer = new LamportSpFastFlowScRingBuffer(bytes, messageAlignment);
            break;
         case MultiProducerSingleConsumer:
            ringBuffer = new LamportMpFastFlowScRingBuffer(bytes, messageAlignment);
            break;
         case RelaxedMultiProducerSingleConsumer:
            ringBuffer = new RelaxedMpFastFlowScRingBuffer(bytes, messageAlignment, DEFAULT_CYCLES);
            break;
         default:
            throw new AssertionError("unsupported case!");
      }
      return ringBuffer;
   }

   private static int messageAlignment(int messageLength) {
      return BytesUtils.nextPowOf2((int) BytesUtils.align(MessageLayout.HEADER_LENGTH + messageLength, MessageLayout.DEFAULT_ALIGNMENT));
   }

   public static int capacity(RingBufferType type, int messages, int messageLength) {
      final int messageAlignment = messageAlignment(messageLength);
      final int bytes = messages * messageAlignment;
      return capacity(type, bytes);
   }

   public enum RingBufferType {
      SingleProducerSingleConsumer, MultiProducerSingleConsumer, RelaxedMultiProducerSingleConsumer
   }

}
