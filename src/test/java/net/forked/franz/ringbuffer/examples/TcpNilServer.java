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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import net.forked.franz.ringbuffer.utils.BytesUtils;
import net.forked.franz.ringbuffer.utils.UnsafeAccess;

public class TcpNilServer {

   /**
    * It is a single threaded, unpinned and unblocking TCP server that read from the first accepted connection on the selected {@code port},
    * using a buffer of {@code readSize}  bytes (aligned to the OS's page size), until forcibly interrupted.
    */
   public static void main(String[] args) throws IOException {
      final int port = 8080;
      final int readSize = UnsafeAccess.UNSAFE.pageSize();
      final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(readSize + UnsafeAccess.UNSAFE.pageSize());
      final int alignedToOSPageIndex = BytesUtils.alignedIndex(byteBuffer, 0, UnsafeAccess.UNSAFE.pageSize());
      byteBuffer.position(alignedToOSPageIndex);
      byteBuffer.limit(byteBuffer.position() + readSize);
      final ByteBuffer networkBuffer = byteBuffer.slice();
      networkBuffer.order(ByteOrder.nativeOrder());
      try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
         serverSocketChannel.bind(new InetSocketAddress(port));
         while (true) {
            System.out.println("waiting the first connection on  " + port + " port...");
            long bytesRead = 0;
            try (SocketChannel socketChannel = serverSocketChannel.accept()) {
               System.out.println("...accepted connection on: " + socketChannel);
               socketChannel.configureBlocking(false);
               boolean endOfStream = false;
               while (!endOfStream) {
                  try {
                     while (!endOfStream && networkBuffer.hasRemaining()) {
                        final int read = socketChannel.read(networkBuffer);
                        if (read == -1) {
                           System.err.println("reached end of stream, close connection!");
                           endOfStream = true;
                        } else {
                           bytesRead += read;
                        }
                     }
                  } finally {
                     networkBuffer.clear();
                  }
               }
            } catch (IOException e) {
               System.err.println(e);
            } finally {
               System.out.println("total read bytes: " + bytesRead);
            }
         }
      }

   }

}
