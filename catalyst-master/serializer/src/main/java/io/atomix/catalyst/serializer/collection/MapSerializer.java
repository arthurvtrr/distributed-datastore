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
package io.atomix.catalyst.serializer.collection;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;

import java.util.Map;
import java.util.Set;

/**
 * Map serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class MapSerializer<T extends Map> implements TypeSerializer<T> {

  /**
   * Creates a new map for deserialization.
   */
  protected abstract T createMap(int size);

  @Override
  @SuppressWarnings("unchecked")
  public void write(T object, BufferOutput buffer, Serializer serializer) {
    buffer.writeUnsignedShort(object.size());
    for (Map.Entry entry : (Set<Map.Entry>) object.entrySet()) {
      serializer.writeObject(entry.getKey(), buffer);
      serializer.writeObject(entry.getValue(), buffer);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T read(Class<T> type, BufferInput buffer, Serializer serializer) {
    int size = buffer.readUnsignedShort();
    T object = createMap(size);
    for (int i = 0; i < size; i++) {
      Object key = serializer.readObject(buffer);
      Object value = serializer.readObject(buffer);
      object.put(key, value);
    }
    return object;
  }

}
