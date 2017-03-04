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
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import net.forked.franz.ringbuffer.MessageConsumer;
import net.forked.franz.ringbuffer.RingBuffer;
import net.forked.franz.ringbuffer.RingBuffers;
import net.forked.franz.ringbuffer.utils.BytesUtils;
import net.forked.franz.ringbuffer.utils.UnsafeAccess;

public class TcpBatchSender {

   private static final int ETHERNET_MTU_PAYLOAD_IP_V4 = 1460;

   /**
    * It is an example of {@link RingBuffer} usage around TCP low-latency smart-batching.<br/>
    * It must be run only after {@link TcpNilServer} TCP server is started.<br/>
    * It performs burst writes of {@code messages} for {@code tests} rounds against a {@code networkRingBuffer} that batches each write against a non-blocking
    * {@link SocketChannel}.<br/>
    * The main advantage of using a {@link RingBuffer} is due to its batch {@link RingBuffer#read} that allows the "real" writer to perform
    * smart-batching writes of {batchSize} bytes depending on how fast the {@link SocketChannel} will be able to cope with the
    * {@link RingBuffer#write} rate.<br/>
    * Using socket options like {@link StandardSocketOptions#TCP_NODELAY} and tuning {@code batchSize} to match the TCP available payload (eg: {@link #ETHERNET_MTU_PAYLOAD_IP_V4}), will
    * allow the real "writer" to send the lowest possible number of packet fragments (avoiding anyway the silly window syndrome) with the lowest possible latency.<br/>
    * To measure the difference of disabling smart-batching is necessary to assign 0 as {@code batchSize}.<br/>
    * Just as a reference, on localhost, with {@code messageSize} = 8 bytes:
    * <ul>
    * <li>{@code batchSize} = 0 bytes: 9 MBytes/sec</li>
    * <li>{@code batchSize} = {@link #ETHERNET_MTU_PAYLOAD_IP_V4} bytes: 250 MBytes/sec (X27 improvement)</li>
    * </ul>
    * Must be considered that beside any throughput improvement is the end-to-end latency that will benefit the most using such technique.
    */
   public static void main(String[] args) throws InterruptedException {

      final RingBuffers.RingBufferType ringBufferType = RingBuffers.RingBufferType.SingleProducerSingleConsumer;
      final boolean tcpNoDelay = false;
      final int tests = 10;
      final int messages = 1_000_000;
      final int messageSize = 8;
      final int batchSize = ETHERNET_MTU_PAYLOAD_IP_V4;
      final int capacity = 32 * 1024;
      final int port = 8080;
      final int messagePollLimit = capacity / messageSize;

      final ByteBuffer networkBuffer;
      if (batchSize == 0) {
         networkBuffer = null;
      } else {
         final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(UnsafeAccess.UNSAFE.pageSize() + batchSize);
         final int alignedToOSPageIndex = BytesUtils.alignedIndex(byteBuffer, 0, UnsafeAccess.UNSAFE.pageSize());
         byteBuffer.position(alignedToOSPageIndex);
         byteBuffer.limit(byteBuffer.position() + batchSize);
         networkBuffer = byteBuffer.slice();
         networkBuffer.order(ByteOrder.nativeOrder());
      }
      final int totalSize = RingBuffers.capacity(ringBufferType, capacity) + UnsafeAccess.UNSAFE.pageSize();
      ByteBuffer ringBufferBytes = ByteBuffer.allocateDirect(totalSize);
      ringBufferBytes.position(BytesUtils.alignedIndex(ringBufferBytes, 0, UnsafeAccess.UNSAFE.pageSize()));
      ringBufferBytes.limit(ringBufferBytes.position() + RingBuffers.capacity(ringBufferType, capacity));
      ringBufferBytes = ringBufferBytes.slice();
      final AtomicLong works = new AtomicLong(0);
      final RingBuffer networkRingBuffer = RingBuffers.with(ringBufferType, ringBufferBytes);
      final Thread batcherThread = new Thread(() -> {
         try (SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(port))) {
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, tcpNoDelay);
            socketChannel.configureBlocking(false);
            final NetworkBatcher batcher = new NetworkBatcher(socketChannel, networkBuffer, networkRingBuffer, messagePollLimit, batchSize);
            long worksDone = 0;
            while (!Thread.currentThread().isInterrupted()) {
               final int work = batcher.doWork();
               if (work > 0) {
                  worksDone += work;
                  works.lazySet(worksDone);
               }
            }
         } catch (IOException e) {
            throw new IllegalStateException(e);
         }

      });
      batcherThread.start();
      for (int t = 0; t < tests; t++) {
         Thread.sleep(2000);
         final long nextTarget = (t + 1) * messages;
         final long start = System.nanoTime();
         for (int i = 0; i < messages; i++) {
            while (networkRingBuffer.write(1, messageSize, (msgType, bytes, offset, length, arg) -> {
            }, null) < 0) {
               LockSupport.parkNanos(1L);
            }
         }
         while (works.get() != nextTarget) {
            LockSupport.parkNanos(1L);
         }
         final long elapsed = System.nanoTime() - start;
         System.out.println(((messages * 1000_000_000L) / elapsed) * messageSize + " bytes/sec");
      }

      batcherThread.interrupt();
      batcherThread.join();
   }

   private static final class NetworkBatcher {

      private final SocketChannel socketChannel;
      private final ByteBuffer batchBuffer;
      private final RingBuffer writeBuffer;
      private final MessageConsumer messageConsumer;
      private final int messagePollLimit;
      private final int maxBatchSize;
      private int batchSize;

      NetworkBatcher(SocketChannel socketChannel,
                     ByteBuffer batchBuffer,
                     RingBuffer writeBuffer,
                     int messagePollLimit,
                     int maxBatchSize) {
         this.socketChannel = socketChannel;
         this.batchBuffer = batchBuffer;
         this.writeBuffer = writeBuffer;
         this.batchSize = 0;
         this.messageConsumer = this::onMessage;
         this.messagePollLimit = messagePollLimit;
         this.maxBatchSize = maxBatchSize;
      }

      private static void writeBytes(SocketChannel socketChannel, ByteBuffer buffer, int index, int length) {
         //write with zero copy directly
         buffer.position(index);
         buffer.limit(index + length);
         try {
            while (buffer.hasRemaining()) {
               socketChannel.write(buffer);
            }
         } catch (IOException e) {
            throw new IllegalStateException(e);
         } finally {
            buffer.clear();
         }
      }

      public int doWork() {
         final int read = this.writeBuffer.read(this.messageConsumer, this.messagePollLimit);
         if (this.batchSize > 0) {
            writeBatch();
         }
         return read;
      }

      private void writeBatch() {
         try {
            batchBuffer.limit(this.batchSize);
            while (batchBuffer.hasRemaining()) {
               socketChannel.write(batchBuffer);
            }
            batchBuffer.clear();
         } catch (Exception e) {
            throw new IllegalStateException(e);
         } finally {
            this.batchSize = 0;
         }
      }

      private void onMessage(int msgTypeId, ByteBuffer buffer, int index, int length) {
         if (this.batchSize > 0 && (this.batchSize + length) > maxBatchSize) {
            writeBatch();
         }
         if (length < maxBatchSize) {
            BytesUtils.copy(buffer, index, this.batchBuffer, this.batchSize, length);
            this.batchSize += length;
         } else {
            //write with zero copy
            writeBytes(socketChannel, buffer, index, length);
         }
      }

   }
}
