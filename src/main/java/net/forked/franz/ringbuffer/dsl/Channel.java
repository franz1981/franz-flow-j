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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.IntFunction;

import net.forked.franz.ringbuffer.RefRingBuffer;
import net.forked.franz.ringbuffer.RingBuffers;

public final class Channel<E, D, C> {

   private final RefRingBuffer<Holder<C>> ringBuffer;

   private Channel(RefRingBuffer<Holder<C>> ringBuffer) {
      this.ringBuffer = ringBuffer;
   }

   public static IntFunction<? extends ByteBuffer> mappedBytesFactory(File file) {
      return size -> {
         try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw"); FileChannel fileChannel = randomAccessFile.getChannel()) {
            final MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            mappedByteBuffer.order(ByteOrder.nativeOrder());
            return mappedByteBuffer;
         } catch (Exception ex) {
            throw new IllegalStateException();
         }
      };
   }

   public static <E, D, C> Channel<E, D, C> newChannel(RingBuffers.RingBufferType type,
                                                       int capacity,
                                                       int averageMarshalledSize,
                                                       IntFunction<? extends ByteBuffer> bytesFactory) {

      final int bytesCapacity = RingBuffers.capacity(type, capacity, averageMarshalledSize);
      final ByteBuffer byteBuffer = bytesFactory.apply(bytesCapacity);
      byteBuffer.order(ByteOrder.nativeOrder());
      final RefRingBuffer<Holder<C>> ringBuffer = RingBuffers.withRef(type, byteBuffer, Holder::new, averageMarshalledSize);
      return new Channel<>(ringBuffer);
   }

   public Subscriber<D, C> newSubscriber(InstanceFactory<? extends D> instanceFactory,
                                         Unmarshaller<? super D> unmarshaller) {
      return new Subscriber<>(this.ringBuffer, unmarshaller, instanceFactory);
   }

   public Publisher<E, C> newPublisher(InstanceMapper<? super E> instanceMapper,
                                       MarshalledBytesPredictor<? super E> marshalledBytesPredictor,
                                       Marshaller<? super E> marshaller) {
      return new Publisher<>(this.ringBuffer, instanceMapper, marshalledBytesPredictor, marshaller);
   }

   public int size() {
      return this.ringBuffer.size();
   }

}
