/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.catalyst.serializer.lang;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;

/**
 * Integer array serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LongArraySerializer implements TypeSerializer<long[]> {

  @Override
  public void write(long[] longs, BufferOutput buffer, Serializer serializer) {
    buffer.writeUnsignedShort(longs.length);
    for (long l : longs) {
      buffer.writeLong(l);
    }
  }

  @Override
  public long[] read(Class<long[]> type, BufferInput buffer, Serializer serializer) {
    long[] longs = new long[buffer.readUnsignedShort()];
    for (int i = 0; i < longs.length; i++) {
      longs[i] = buffer.readLong();
    }
    return longs;
  }

}
