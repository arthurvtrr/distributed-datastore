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
package io.atomix.catalyst.serializer.util;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;
import io.atomix.catalyst.util.reference.ReferenceCounted;

/**
 * Provides pooled object serialization.
 * <p>
 * The {@code PooledSerializer} is provided as a base class for {@link ReferenceCounted} object serializers. When objects
 * are deserialized by pooled serializers, available objects will be acquired via {@link PooledTypeSerializer#acquire(Class)}
 * rather than being constructed new.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class PooledTypeSerializer<T extends ReferenceCounted<T>> implements TypeSerializer<T> {

  @Override
  public T read(Class<T> type, BufferInput buffer, Serializer serializer) {
    T object = acquire(type);
    read(object, buffer, serializer);
    return object;
  }

  /**
   * Acquires a reference.
   *
   * @param type The reference type.
   * @return The acquired reference.
   */
  protected abstract T acquire(Class<T> type);

  /**
   * Reads the object from the given buffer.
   *
   * @param object The object to read.
   * @param buffer The buffer from which to read the object.
   * @param serializer The Catalyst serializer.
   */
  protected abstract void read(T object, BufferInput buffer, Serializer serializer);

}
