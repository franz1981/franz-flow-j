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

/**
 * Ring-buffer for the concurrent exchanging of binary encoded messages from producer to consumer in a FIFO manner.
 */
public interface RefRingBuffer<T> extends RingBuffer {

   /**
    * Get the capacity in references of the ring-buffer.
    *
    * @return the capacity in references of the ring-buffer.
    */
   int refCapacity();

   /**
    * Non-blocking write of a message to an underlying ring-buffer.
    *
    * @param msgTypeId  type of the message.
    * @param length     of the message content in bytes.
    * @param translator zero copy translator to put data in the ringBuffer
    * @return the producerPosition if written to the ring-buffer,or <0 if insufficient space exists.
    * @throws IllegalArgumentException if the length is greater than {@link RingBuffer#maxMessageLength()}
    */
   <A1, A2> long write(final int msgTypeId,
                       final int length,
                       final MessageRefTranslator<? super T, ? super A1, ? super A2> translator,
                       A1 arg1,
                       A2 arg2);

   /**
    * Read as many messages as are available from the ring buffer.
    *
    * @param consumer to be called for processing each message in turn.
    * @return the number of messages that have been processed.
    */
   int read(MessageRefConsumer<? super T> consumer);

   /**
    * Read as many messages as are available from the ring buffer to up a supplied maximum.
    *
    * @param consumer          to be called for processing each message in turn.
    * @param messageCountLimit the number of messages will be read in a single invocation.
    * @return the number of messages that have been processed.
    */
   int read(MessageRefConsumer<? super T> consumer, int messageCountLimit);

}
