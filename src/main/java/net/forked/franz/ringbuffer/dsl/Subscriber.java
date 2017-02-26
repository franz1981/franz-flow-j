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
import java.util.function.BiConsumer;

import net.forked.franz.ringbuffer.MessageRefConsumer;
import net.forked.franz.ringbuffer.RefRingBuffer;

public final class Subscriber<T, C> {

   private final RefRingBuffer<Holder<C>> ringBuffer;
   private final Unmarshaller<? super T> unmarshaller;
   private final InstanceFactory<? extends T> instanceFactory;
   private final MessageConsumerAdapter consumerAdapter;

   Subscriber(RefRingBuffer<Holder<C>> ringBuffer,
              Unmarshaller<? super T> unmarshaller,
              InstanceFactory<? extends T> instanceFactory) {
      this.ringBuffer = ringBuffer;
      this.unmarshaller = unmarshaller;
      this.instanceFactory = instanceFactory;
      this.consumerAdapter = new MessageConsumerAdapter();
   }

   public int poll(BiConsumer<T, C> onMessage) {
      return poll(onMessage, Integer.MAX_VALUE);
   }

   public int poll(BiConsumer<T, C> onMessage, int count) {
      try {
         consumerAdapter.onMessage = onMessage;
         return this.ringBuffer.read(consumerAdapter, count);
      } finally {
         consumerAdapter.onMessage = null;
      }
   }

   private final class MessageConsumerAdapter implements MessageRefConsumer<Holder<C>> {

      private BiConsumer<T, C> onMessage;

      @Override
      public void accept(Holder<C> ref, int msgTypeId, ByteBuffer buffer, int index, int length) {
         try {
            final T instance = instanceFactory.instance(msgTypeId);
            Subscriber.this.unmarshaller.convert(buffer, index, length, instance);
            onMessage.accept(instance, ref.instance);
         } finally {
            ref.instance = null;
         }
      }
   }
}
