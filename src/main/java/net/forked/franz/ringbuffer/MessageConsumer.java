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
 * Callback interface for processing of messages that are read from a buffer.
 */
@FunctionalInterface
public interface MessageConsumer {

   /**
    * Called for the processing of each message read from a buffer in turn.
    *
    * @param msgTypeId type of the message.
    * @param buffer    containing the message content.
    * @param index     at which the message content begins.
    * @param length    in bytes of message content.
    */
   void accept(int msgTypeId, ByteBuffer buffer, int index, int length);
}