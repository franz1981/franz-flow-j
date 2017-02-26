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

package net.forked.franz.ringbuffer.dsl;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import net.forked.franz.ringbuffer.RingBuffers;

public class ChannelTest {

   public static void main(String[] args) throws InterruptedException, IOException {
      final File tmpFile = File.createTempFile("channel", ".dat");
      tmpFile.deleteOnExit();
      final RingBuffers.RingBufferType type = RingBuffers.RingBufferType.MultiProducerSingleConsumer;
      final int messages = 10_000_000;
      final int tests = 10;
      final int capacity = 1024;
      final int averageMessageSize = MessageId.SIZE;
      final Channel<MessageId, MessageId, Callback> channel = Channel.newChannel(type, capacity, averageMessageSize, Channel.mappedBytesFactory(tmpFile));
      final CountDownLatch consumerStarted = new CountDownLatch(1);
      final Thread consumer = new Thread(() -> {
         final MessageId instance = new MessageId();
         final Subscriber<MessageId, Callback> subscriber = channel.newSubscriber(id -> instance, (src, srcOffset, srcLength, dstObj) -> dstObj.value = src.getLong(srcOffset));
         consumerStarted.countDown();
         long count = messages * tests;
         while (count > 0) {
            final int read = pollWith(subscriber);
            count -= read;
         }
      });
      consumer.start();
      consumerStarted.await();
      //could be a thread local variable in a real case
      long messageId = 0;
      final Publisher<MessageId, Callback> publisher = channel.newPublisher(instance -> MessageId.MSG_TYPE_ID, instance -> MessageId.SIZE, (srcObj, dstByteBuffer, dstOffset, dstLength) -> dstByteBuffer.putLong(dstOffset, srcObj.value));
      final MessageId message = new MessageId();
      final Callback callback = new Callback();
      for (int t = 0; t < tests; t++) {
         long start = System.nanoTime();
         for (int i = 0; i < messages; i++) {
            message.value = messageId;
            publishWith(publisher, message, callback);
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

   private static void publishWith(Publisher<MessageId, Callback> publisher, MessageId messageId, Callback callback) {
      while (!publisher.tryPublish(messageId, callback)) {

      }
   }

   private static int pollWith(Subscriber<MessageId, Callback> subscriber) {
      final int read = subscriber.poll((messageId, callback) -> callback.onFinished(messageId.value));
      return read;
   }

   private static final class MessageId {

      private static final int MSG_TYPE_ID = 1;
      private static final int SIZE = Long.BYTES;

      long value;
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
