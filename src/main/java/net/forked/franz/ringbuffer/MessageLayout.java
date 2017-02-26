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
 * Description of the message layout in the a {@link RingBuffer}.
 */
final class MessageLayout {

   /**
    * Header length made up of fields for length, type, and then the encoded message.
    */
   public static final int HEADER_LENGTH = Integer.BYTES * 2;
   /**
    * Alignment as a multiple of bytes for each message.
    */
   public static final int DEFAULT_ALIGNMENT = HEADER_LENGTH;

   private MessageLayout() {

   }

   /**
    * The offset from the beginning of a message at which the message length field begins.
    *
    * @param msgOffset beginning index of the message.
    * @return offset from the beginning of a message at which the type field begins.
    */
   public static int lengthOffset(final int msgOffset) {
      return msgOffset;
   }

   /**
    * The offset from the beginning of a message at which the message type field begins.
    *
    * @param msgOffset beginning index of the message.
    * @return offset from the beginning of a message at which the type field begins.
    */
   public static int msgTypeOffset(final int msgOffset) {
      return msgOffset + Integer.BYTES;
   }

   /**
    * The offset from the beginning of a message at which the message content begins.
    *
    * @param msgOffset beginning index of the message.
    * @return offset from the beginning of a message at which the encoded message begins.
    */
   public static int msgContentOffset(final int msgOffset) {
      return msgOffset + HEADER_LENGTH;
   }

   /**
    * Packs header.
    *
    * @param msgTypeId of the message stored in the message
    * @param length    length of the message
    * @return the fields combined into a long.
    */
   public static long packHeaderWith(final int msgTypeId, final int length) {
      return ((msgTypeId & 0xFFFF_FFFFL) << 32) | (length & 0xFFFF_FFFFL);
   }

   /**
    * Extract the message length field from a word representing the header.
    *
    * @param header containing both fields.
    * @return the length field from the header.
    */
   public static int length(final long header) {
      return (int) header;
   }

   public static int msgTypeId(final long header) {
      return (int) (header >>> 32);
   }

   /**
    * Check that and message type id is in the valid range.
    *
    * @param msgTypeId to be checked.
    * @throws IllegalArgumentException if the id is not valid.
    */
   public static void checkMsgTypeId(final int msgTypeId) {
      if (msgTypeId < 1) {
         throw new IllegalArgumentException("Message type id must be greater than zero, msgTypeId" + msgTypeId);
      }
   }

}