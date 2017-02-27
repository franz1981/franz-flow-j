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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class IpcReader {

   public static void main(String[] args) throws IOException {
      final int tests = 10;
      final int messages = 100_000_000;
      final int messageSize = Long.BYTES;
      final int requiredCapacity = 128 * 1024;
      final int maxBatchSize = requiredCapacity / messageSize;
      final File sharedFolder = new File("/dev/shm");
      final String sharedFileName = "shared.ipc";
      final RingBuffers.RingBufferType ringBufferType = RingBuffers.RingBufferType.SingleProducerSingleConsumer;
      final File sharedFile = new File(sharedFolder, sharedFileName);
      final int capacity = RingBuffers.capacity(requiredCapacity);
      final MappedByteBuffer bytes = new RandomAccessFile(sharedFile, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, capacity);
      bytes.order(ByteOrder.nativeOrder());
      final RingBuffer ringBuffer = RingBuffers.with(ringBufferType, bytes);
      long count = messages * tests;
      while (count > 0) {
         final int read = ringBuffer.read((msgTypeId, buffer, index, length) -> {
         }, maxBatchSize);
         count -= read;
      }
   }
}
