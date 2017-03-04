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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class RingBufferTest {

   public static void main(String[] args) throws Exception {
      final RingBuffers.RingBufferType type = RingBuffers.RingBufferType.SingleProducerSingleConsumer;
      final int messages = 10_000_000;
      final int tests = 10;
      final File file = Files.createTempFile("rb", ".bin").toFile();
      file.deleteOnExit();
      final int capacity = RingBuffers.capacity(type, 1024, Long.BYTES);
      final MappedByteBuffer bytes = new RandomAccessFile(file, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, capacity);
      bytes.order(ByteOrder.nativeOrder());
      final RefRingBuffer<WriteRequest> ringBuffer = RingBuffers.withRef(type, bytes, WriteRequest::new, Long.BYTES);
      final CountDownLatch consumerStarted = new CountDownLatch(1);
      final Thread consumer = new Thread(() -> {
         consumerStarted.countDown();
         long count = messages * tests;
         while (count > 0) {
            final int read = ringBuffer.read(RingBufferTest::readMessage);
            count -= read;
         }
      });
      consumer.start();
      consumerStarted.await();
      //could be a thread local variable in a real case
      long messageId = 0;
      final LongValue messageIdHolder = new LongValue();
      final Callback callback = new Callback();
      for (int t = 0; t < tests; t++) {
         long start = System.nanoTime();
         for (int i = 0; i < messages; i++) {
            messageIdHolder.value = messageId;
            writeMessage(ringBuffer, messageIdHolder, callback);
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

   private static void writeMessage(RefRingBuffer<WriteRequest> refRingBuffer, LongValue messageId, Callback callback) {
      while (refRingBuffer.write(1, Long.BYTES, (ref, msgType, bytes, offset, length, id, clbk) -> {
         bytes.putLong(offset, id.value);
         ref.callback = clbk;
      }, messageId, callback) < 0) {
         //busy spin
      }
   }

   private static void readMessage(WriteRequest writeRequest, int msgTypeId, ByteBuffer buffer, int index, int length) {
      try {
         final long currentMessageId = buffer.getLong(index);
         writeRequest.callback.onFinished(currentMessageId);
      } finally {
         writeRequest.callback = null;
      }
   }

   private static final class WriteRequest {

      private Callback callback = null;
   }

   private static final class LongValue {

      private long value;
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
