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

import java.nio.ByteBuffer;

/**
 * Ring-buffer for the concurrent exchanging of binary encoded messages from producer to consumer in a FIFO manner.
 */
public interface RingBuffer {

   long FULL = -2;

   long BACK_PRESSURED = -1;

   /**
    * The size in bytes of the messages alignment.
    *
    * @return The size in bytes of the message alignment.
    */
   int messageAlignment();

   /**
    * The size in bytes of the messages header.
    *
    * @return The size in bytes of the messages header.
    */
   int headerSize();

   /**
    * Get the capacity in bytes of the ring-buffer.
    *
    * @return Get the capacity in bytes of the ring-buffer.
    */
   int capacity();

   /**
    * Non-blocking write of an message to an underlying ring-buffer.
    *
    * @param msgTypeId type of the message.
    * @param srcBuffer containing the binary message content.
    * @param srcIndex  at which the message content begins.
    * @param length    of the message content in bytes.
    * @return the producerPosition if written to the ring-buffer,or <0 if insufficient space exists.
    * @throws IllegalArgumentException if the length is greater than {@link RingBuffer#maxMessageLength()}
    */
   long write(int msgTypeId, ByteBuffer srcBuffer, int srcIndex, int length);

   /**
    * Non-blocking write of an message to an underlying ring-buffer.
    *
    * @param msgTypeId  type of the message.
    * @param length     of the message content in bytes.
    * @param translator zero copy translator to put data in the ringBuffer
    * @return the producerPosition if written to the ring-buffer,or <0 if insufficient space exists.
    * @throws IllegalArgumentException if the length is greater than {@link RingBuffer#maxMessageLength()}
    */
   <A> long write(final int msgTypeId, final int length, final MessageTranslator<? super A> translator, A arg);

   /**
    * Read as many messages as are available from the ring buffer.
    *
    * @param consumer to be called for processing each message in turn.
    * @return the number of messages that have been processed.
    */
   int read(MessageConsumer consumer);

   /**
    * Read as many messages as are available from the ring buffer to up a supplied maximum.
    *
    * @param consumer          to be called for processing each message in turn.
    * @param messageCountLimit the number of messages will be read in a single invocation.
    * @return the number of messages that have been processed.
    */
   int read(MessageConsumer consumer, int messageCountLimit);

   /**
    * The maximum message length in bytes supported by the underlying ring buffer.
    *
    * @return the maximum message length in bytes supported by the underlying ring buffer.
    */
   int maxMessageLength();

   /**
    * Get the underlying buffer used by the RingBuffer for storage.
    *
    * @return the underlying buffer used by the RingBuffer for storage.
    */
   ByteBuffer buffer();

   /**
    * The offset in bytes from start up of the producers.  The figure includes the headers.
    * This is the range they are working with but could still be in the act of working with.
    *
    * @return number of bytes produced by the producers in claimed space.
    */
   long producerPosition();

   /**
    * The offset in bytes from start up for the consumers.  The figure includes the headers.
    *
    * @return the count of bytes consumed by the consumers.
    */
   long consumerPosition();

   /**
    * Size of the backlog of bytes in the buffer between producers and consumers. The figure includes the size of headers.
    *
    * @return size of the backlog of bytes in the buffer between producers and consumers.
    */
   int size();

}