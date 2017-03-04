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

   static long readMessages = 0;
   static long failedRead = 0;
   static long success = 0;

   public static void main(String[] args) throws IOException {
      final int requiredCapacity = 1024 * 1024;
      final int maxBatchSize = 1024;
      final File sharedFolder = new File("/dev/shm");
      final String sharedFileName = "shared.ipc";
      final RingBuffers.RingBufferType ringBufferType = RingBuffers.RingBufferType.RelaxedMultiProducerSingleConsumer;
      final File sharedFile = new File(sharedFolder, sharedFileName);
      sharedFile.delete();
      sharedFile.deleteOnExit();
      final int capacity = RingBuffers.capacity(ringBufferType, requiredCapacity);
      final MappedByteBuffer bytes = new RandomAccessFile(sharedFile, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, capacity);
      bytes.order(ByteOrder.nativeOrder());
      final RingBuffer ringBuffer = RingBuffers.with(ringBufferType, bytes);
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
         if (success > 0) {
            System.out.format("avg batch reads:%d %d/%d failed reads\n", readMessages / success, failedRead, readMessages);
         }
      }));
      while (true) {
         final int read = ringBuffer.read((msgTypeId, buffer, index, length) -> {
         }, maxBatchSize);
         if (read == 0) {
            failedRead++;
         } else {
            success++;
            readMessages += read;
         }
      }

   }
}
