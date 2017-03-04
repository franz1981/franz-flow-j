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

package net.forked.franz.ringbuffer.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import net.forked.franz.ringbuffer.MessageConsumer;
import net.forked.franz.ringbuffer.RingBuffer;
import net.forked.franz.ringbuffer.RingBuffers;
import net.forked.franz.ringbuffer.utils.BytesUtils;
import net.forked.franz.ringbuffer.utils.UnsafeAccess;

public class NioBatchPWrite {

   /**
    * It is an example of {@link RingBuffer} usage around durable journaling.<br/>
    * It performs burst writes of {@code messages} for {@code tests} rounds against a {@code fileRingBuffer} that batches each write against a
    * {@link FileChannel}.<br/>
    * The main advantage of using a {@link RingBuffer} is due to its batch {@link RingBuffer#read} that allows the "real" writer to perform
    * durable commit using {@link FileChannel#force} of variable size depending on how fast the {@link FileChannel} will be able to cope with the
    * {@link RingBuffer#write} rate.<br/>
    * To measure the difference of using smart-batching instead of {@link FileChannel#force} each message is necessary to assign 1 as {@code maxUnflushedMessages}.<br/>
    * Just as a reference, on an SSD, with {@code messageSize} = 8 bytes:
    * <ul>
    * <li>{@code maxUnflushedMessages} = 1 msg: 10 KBytes/sec</li>
    * <li>{@code maxUnflushedMessages} = 4096 msgs: 7 MBytes/sec (X700 improvement)</li>
    * </ul>
    */
   public static void main(String[] args) throws InterruptedException, IOException {
      final Path parentDirectory = Paths.get("./");
      final RingBuffers.RingBufferType ringBufferType = RingBuffers.RingBufferType.SingleProducerSingleConsumer;
      final int tests = 10;
      final int messages = 20_000;
      final int messageSize = 8;
      final int capacity = 32 * 1024;
      final int maxUnflushedMessages = capacity / messageSize;

      final int totalSize = RingBuffers.capacity(ringBufferType, capacity) + UnsafeAccess.UNSAFE.pageSize();
      ByteBuffer ringBufferBytes = ByteBuffer.allocateDirect(totalSize);
      ringBufferBytes.position(BytesUtils.alignedIndex(ringBufferBytes, 0, UnsafeAccess.UNSAFE.pageSize()));
      ringBufferBytes.limit(ringBufferBytes.position() + RingBuffers.capacity(ringBufferType, capacity));
      ringBufferBytes = ringBufferBytes.slice();
      final RingBuffer fileRingBuffer = RingBuffers.with(ringBufferType, ringBufferBytes);
      final Path tmpFile;
      try {
         tmpFile = Files.createTempFile(parentDirectory, "batch", ".dat");
         tmpFile.toFile().deleteOnExit();
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
      try (FileChannel fileChannel = FileChannel.open(tmpFile, StandardOpenOption.WRITE)) {
         final NioPWriteBatcher batcher = new NioPWriteBatcher(fileChannel, 0, fileRingBuffer, maxUnflushedMessages);
         final Thread batcherThread = new Thread(() -> {
            try (FileLock fileLock = fileChannel.lock()) {
               while (!Thread.currentThread().isInterrupted()) {
                  batcher.doWork();
               }
            } catch (IOException e) {
               throw new IllegalStateException(e);
            }
         });
         final long fileSize = messageSize * messages;
         batcherThread.start();
         for (int t = 0; t < tests; t++) {
            Thread.sleep(2000);
            //start writing from the the beginning of the file
            batcher.position(0);
            final long start = System.nanoTime();
            for (int i = 0; i < messages; i++) {
               while (fileRingBuffer.write(1, messageSize, (msgType, bytes, offset, length, arg) -> {
               }, null) < 0) {
                  LockSupport.parkNanos(1L);
               }
            }
            //wait until finished the last flush on the file
            while (batcher.lastFlushedPosition() != fileSize) {
               LockSupport.parkNanos(1L);
            }
            final long elapsed = System.nanoTime() - start;
            System.out.println(((messages * 1000_000_000L) / elapsed) * messageSize + " bytes/sec");
         }
         batcherThread.interrupt();
         batcherThread.join();
      }
   }

   private static final class NioPWriteBatcher {

      private final FileChannel fileChannel;
      private final RingBuffer writeBuffer;
      private final MessageConsumer messageConsumer;
      private final int messagePollLimit;
      private final AtomicLong lastFlushedPosition;
      private long position;

      NioPWriteBatcher(FileChannel fileChannel, long position, RingBuffer writeBuffer, int messagePollLimit) {
         this.fileChannel = fileChannel;
         this.position = position;
         this.writeBuffer = writeBuffer;
         this.messageConsumer = this::onMessage;
         this.messagePollLimit = messagePollLimit;
         this.lastFlushedPosition = new AtomicLong(position);
      }

      public long lastFlushedPosition() {
         return this.lastFlushedPosition.get();
      }

      void position(long position) {
         this.position = position;
         this.lastFlushedPosition.lazySet(position);
      }

      public int doWork() {
         final int read = this.writeBuffer.read(this.messageConsumer, this.messagePollLimit);
         if (read > 0) {
            try {
               this.fileChannel.force(false);
            } catch (IOException e) {
               throw new IllegalStateException(e);
            } finally {
               lastFlushedPosition.lazySet(this.position);
            }
         }
         return read;
      }

      private void onMessage(int msgTypeId, ByteBuffer buffer, int index, int length) {
         buffer.position(index);
         buffer.limit(index + length);
         try {
            this.fileChannel.write(buffer, position);
            position += length;
         } catch (IOException e) {
            throw new IllegalStateException(e);
         } finally {
            buffer.clear();
         }
      }

   }
}
