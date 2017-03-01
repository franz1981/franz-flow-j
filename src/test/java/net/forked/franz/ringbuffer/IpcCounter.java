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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import net.forked.franz.ringbuffer.utils.BytesUtils;

public class IpcCounter {

   public static void main(String[] args) throws IOException {
      final File sharedFolder = new File("/dev/shm");
      final String sharedFileName = "shared.ipc";
      final RingBuffers.RingBufferType ringBufferType = RingBuffers.RingBufferType.MultiProducerSingleConsumer;
      final File sharedFile = new File(sharedFolder, sharedFileName);
      while (!sharedFile.exists()) {
         System.out.println("...waiting that " + sharedFile + " will be created...");
         LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
      }
      final MappedByteBuffer bytes = new RandomAccessFile(sharedFile, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, sharedFile.length());
      bytes.order(ByteOrder.nativeOrder());
      final RingBuffer ringBuffer = RingBuffers.with(ringBufferType, bytes);
      final int messageSize = (int) BytesUtils.align(ringBuffer.headerSize() + Long.BYTES, ringBuffer.messageAlignment());
      long lastTime = System.currentTimeMillis();
      long lastBytes = ringBuffer.consumerPosition();
      while (true) {
         LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
         final long time = System.currentTimeMillis();
         final long bytez = ringBuffer.consumerPosition();
         final long elapsedTime = time - lastTime;
         final long bytesConsumed = bytez - lastBytes;
         System.out.format("elapsed %dms\t%,d messages\t%,d bytes%n", elapsedTime, bytesConsumed / messageSize, bytesConsumed);
         lastTime = time;
         lastBytes = bytez;
      }
   }

}
