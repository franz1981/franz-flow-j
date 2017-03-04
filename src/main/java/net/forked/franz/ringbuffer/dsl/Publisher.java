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

package net.forked.franz.ringbuffer.dsl;

import java.nio.ByteBuffer;

import net.forked.franz.ringbuffer.MessageRefTranslator;
import net.forked.franz.ringbuffer.RefRingBuffer;

public final class Publisher<T, C> {

   private final RefRingBuffer<Holder<C>> ringBuffer;
   private final MarshalledBytesPredictor<? super T> marshalledBytesPredictor;
   private final MessageRefTranslatorAdapter<T, C> translatorAdapter;
   private final InstanceMapper<? super T> instanceMapper;

   Publisher(RefRingBuffer<Holder<C>> ringBuffer,
             InstanceMapper<? super T> instanceMapper,
             MarshalledBytesPredictor<? super T> marshalledBytesPredictor,
             Marshaller<? super T> marshaller) {
      this.ringBuffer = ringBuffer;
      this.translatorAdapter = new MessageRefTranslatorAdapter<>(marshaller);
      this.instanceMapper = instanceMapper;
      this.marshalledBytesPredictor = marshalledBytesPredictor;
   }

   public boolean tryPublish(T obj, C callback) {
      final int msgTypeId = this.instanceMapper.typeId(obj);
      final int length = this.marshalledBytesPredictor.bytes(obj);
      return this.ringBuffer.write(msgTypeId, length, translatorAdapter, obj, callback) >= 0;
   }

   private static final class MessageRefTranslatorAdapter<T, C> implements MessageRefTranslator<Holder<C>, T, C> {

      private final Marshaller<? super T> marshaller;

      MessageRefTranslatorAdapter(Marshaller<? super T> marshaller) {
         this.marshaller = marshaller;
      }

      @Override
      public void translate(Holder<C> ref,
                            int msgType,
                            ByteBuffer bytes,
                            int offset,
                            int length,
                            T srcObj,
                            C callback) {
         marshaller.convert(srcObj, bytes, offset, length);
         ref.instance = callback;
      }
   }

}
