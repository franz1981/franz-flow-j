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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

public class ExecutorServiceTest {

   public static void main(String[] args) throws Exception {
      final int messages = 10_000_000;
      final int tests = 10;

      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      //added to be sure the executor service thread is already up and running
      executorService.submit(() -> {
      }).get();
      long messageId = 0;
      final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
      final Callback callback = new Callback();
      for (int t = 0; t < tests; t++) {
         long start = System.nanoTime();
         for (int i = 0; i < messages; i++) {
            writeMessage(executorService, allocator, messageId, callback);
            messageId++;
         }
         while (callback.messageId() != (messageId - 1)) {
            LockSupport.parkNanos(1L);
         }
         //wait until the last callback of the test is processed
         final long elapsed = System.nanoTime() - start;
         System.out.println((messages * 1000000000L) / elapsed + " ops/sec");
      }
      executorService.shutdown();
   }

   private static void writeMessage(ExecutorService executorService,
                                    ByteBufAllocator allocator,
                                    long messageId,
                                    Callback callback) {
      final ByteBuf byteBuf = allocator.directBuffer(Long.BYTES, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      byteBuf.setLong(0, messageId);
      //the Runnable lambda is capturing byteBuf and callback
      executorService.submit(() -> {
         try {
            final long currentMessageId = byteBuf.getLong(0);
            callback.onFinished(currentMessageId);
         } finally {
            byteBuf.release();
         }
      });
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
