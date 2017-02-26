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

import java.nio.ByteOrder;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

public class ConcurrentQueueTest {

   public static void main(String[] args) throws Exception {
      final boolean blocking = false;
      final int messages = 10_000_000;
      final int tests = 10;
      final int capapcity = 1024;
      final Queue<WriteRequest> writeRequests = blocking ? new ArrayBlockingQueue<WriteRequest>(capapcity) : new ConcurrentLinkedQueue<>();
      final CountDownLatch consumerStarted = new CountDownLatch(1);
      final Thread consumer = new Thread(() -> {
         consumerStarted.countDown();
         long count = messages * tests;
         while (count > 0) {
            int read = 0;
            WriteRequest writeRequest;
            while ((writeRequest = writeRequests.poll()) != null) {
               readMessage(writeRequest);
               read++;
            }
            count -= read;
         }
      });
      consumer.start();
      consumerStarted.await();
      //could be a thread local variable in a real case
      long messageId = 0;
      final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
      final Callback callback = new Callback();
      for (int t = 0; t < tests; t++) {
         long start = System.nanoTime();
         for (int i = 0; i < messages; i++) {
            writeMessage(writeRequests, allocator, messageId, callback);
            messageId++;
         }
         while (callback.messageId() != (messageId - 1)) {
            LockSupport.parkNanos(1L);
         }
         //wait until the last callback of the test is processed
         final long elapsed = System.nanoTime() - start;
         System.out.println((messages * 1000000000L) / elapsed + " ops/sec");

      }
   }

   private static void writeMessage(Queue<WriteRequest> writeRequests,
                                    ByteBufAllocator allocator,
                                    long messageId,
                                    Callback callback) {
      final ByteBuf byteBuf = allocator.directBuffer(Long.BYTES, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      byteBuf.setLong(0, messageId);
      //the Runnable lambda is capturing byteBuf and callback
      final WriteRequest writeRequest = new WriteRequest(byteBuf, callback);
      while (!writeRequests.offer(writeRequest)) {

      }
   }

   private static void readMessage(WriteRequest writeRequest) {
      final ByteBuf messageBytes = writeRequest.byteBuf;
      try {
         final long currentMessageId = messageBytes.getLong(0);
         writeRequest.callback.onFinished(currentMessageId);
      } finally {
         messageBytes.release();
      }
   }

   private static final class WriteRequest {

      final Callback callback;
      final ByteBuf byteBuf;

      WriteRequest(ByteBuf byteBuf, Callback callback) {
         this.byteBuf = byteBuf;
         this.callback = callback;
      }

   }

   private static final class Callback {

      private final AtomicLong messageId;

      Callback() {
         messageId = new AtomicLong();
      }

      public void onFinished(long messageId) {
         this.messageId.lazySet(messageId);
      }

      public long messageId() {
         return messageId.get();
      }
   }
}
