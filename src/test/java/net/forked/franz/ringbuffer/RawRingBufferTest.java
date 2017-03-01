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
import java.nio.ByteOrder;
import java.util.concurrent.locks.LockSupport;

public class RawRingBufferTest {

   private static final int DEFAULT_MSG_TYPE_ID = 1;
   private static final int DEFAULT_MSG_LENGTH = Long.BYTES;
   private static final long DEFAULT_MSG_CONTENT = Long.MIN_VALUE;

   public static void main(String[] args) throws Exception {
      final RingBuffers.RingBufferType type = RingBuffers.RingBufferType.MultiProducerSingleConsumer;
      final int messages = 100_000_000;
      final int tests = 10;
      final int capacity = RingBuffers.capacity(128 * 1024);
      final int batchSize = 128;
      final ByteBuffer bytes = ByteBuffer.allocateDirect(capacity);
      bytes.order(ByteOrder.nativeOrder());
      final RingBuffer ringBuffer = RingBuffers.with(type, bytes);
      final Thread consumer = new Thread(() -> {
         long readMessages = 0;
         long failedRead = 0;
         long success = 0;
         final long totalMessages = messages * tests;
         while (readMessages < totalMessages) {
            final int read = ringBuffer.read((msgTypeId, buffer, index, length) -> {
               if (length != DEFAULT_MSG_LENGTH && msgTypeId != DEFAULT_MSG_TYPE_ID && buffer.getLong(index) != DEFAULT_MSG_CONTENT) {
                  throw new IllegalStateException("INCONSISTENT MESSAGE!");
               }
            }, batchSize);
            if (read == 0) {
               failedRead++;
            } else {
               success++;
               readMessages += read;
            }
         }
         System.out.format("avg batch reads:%d %d/%d failed reads\n", readMessages / success, failedRead, totalMessages);
      });
      consumer.start();
      for (int t = 0; t < tests; t++) {
         long totalTry = 0;
         long writtenPosition = 0;
         long start = System.nanoTime();
         for (int i = 0; i < messages; i++) {

            while ((writtenPosition = ringBuffer.write(DEFAULT_MSG_TYPE_ID, DEFAULT_MSG_LENGTH, (msgId, message, index, len, nil) -> {
               message.putLong(index, DEFAULT_MSG_CONTENT);
            }, null)) < 0) {
               totalTry++;
            }
            totalTry++;
         }
         final long endProduceTime = System.nanoTime();
         while (ringBuffer.consumerPosition() < writtenPosition) {
            LockSupport.parkNanos(1L);
         }
         final long endTime = System.nanoTime();
         final long waitNanos = endTime - endProduceTime;
         final long elapsedNanos = endTime - start;
         System.out.format("[%d] %dM ops/sec %d/%d failed tries end latency:%d ns\n", Thread.currentThread().getId(), (messages * 1000L) / elapsedNanos, totalTry - messages, messages, waitNanos);

      }
   }
}
