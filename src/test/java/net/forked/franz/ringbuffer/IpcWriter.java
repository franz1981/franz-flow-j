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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.LockSupport;

public class IpcWriter {

   private static final int DEFAULT_MSG_TYPE_ID = 1;
   private static final int DEFAULT_MSG_LENGTH = Long.BYTES;
   private static final long DEFAULT_MSG_CONTENT = Long.MIN_VALUE;

   public static void main(String[] args) throws IOException {
      final int tests = 10;
      final int messages = 100_000_000;
      final File sharedFolder = new File("/dev/shm");
      final String sharedFileName = "shared.ipc";
      final RingBuffers.RingBufferType ringBufferType = RingBuffers.RingBufferType.MultiProducerSingleConsumer;
      final File sharedFile = new File(sharedFolder, sharedFileName);
      if (!sharedFile.exists()) {
         throw new IllegalStateException("shared file doesn't exists!");
      }
      final FileChannel fileChannel = new RandomAccessFile(new File(sharedFolder, sharedFileName), "rw").getChannel();
      final int capacity = (int) fileChannel.size();
      if (capacity < 0) {
         throw new IllegalStateException("shared file too big!");
      }
      final MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
      final RingBuffer ringBuffer = RingBuffers.with(ringBufferType, mappedByteBuffer);
      for (int t = 0; t < tests; t++) {
         final long start = System.nanoTime();
         long writtenPosition = 0;
         for (int m = 0; m < messages; m++) {
            while ((writtenPosition = ringBuffer.write(DEFAULT_MSG_TYPE_ID, DEFAULT_MSG_LENGTH, (msgType, bytes, offset, length, arg) -> bytes.putLong(offset, DEFAULT_MSG_CONTENT), null)) < 0) {
               //busy spin
            }
         }
         while (ringBuffer.consumerPosition() < writtenPosition) {
            LockSupport.parkNanos(1L);
         }
         final long elapsed = System.nanoTime() - start;
         System.out.println((messages * 1000_000_000L / elapsed) + " msg/sec");
      }

   }

}
