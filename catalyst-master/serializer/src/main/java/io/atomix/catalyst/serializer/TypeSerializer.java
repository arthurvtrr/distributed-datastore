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
package io.atomix.catalyst.serializer;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;

/**
 * Provides custom object serialization.
 * <p>
 * This interface can be implemented to provide custom serialization for objects of a given type.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface TypeSerializer<T> {

  /**
   * Writes the object to the given buffer.
   *
   * @param object The object to write.
   * @param buffer The buffer to which to write the object.
   * @param serializer The Catalyst serializer.
   */
  void write(T object, BufferOutput buffer, Serializer serializer);

  /**
   * Reads the object from the given buffer.
   *
   * @param type The type to read.
   * @param buffer The buffer from which to read the object.
   * @return The read object.
   * @param serializer The Catalyst serializer.
   */
  T read(Class<T> type, BufferInput buffer, Serializer serializer);

}
