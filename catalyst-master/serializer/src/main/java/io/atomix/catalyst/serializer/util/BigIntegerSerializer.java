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
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Big decimal serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BigIntegerSerializer implements TypeSerializer<BigInteger> {

  @Override
  public void write(BigInteger object, BufferOutput buffer, Serializer serializer) {
    byte[] bytes = object.toString().getBytes(StandardCharsets.UTF_8);
    buffer.writeInt(bytes.length).write(bytes);
  }

  @Override
  public BigInteger read(Class<BigInteger> type, BufferInput buffer, Serializer serializer) {
    byte[] bytes = new byte[buffer.readInt()];
    buffer.read(bytes);
    return new BigInteger(new String(bytes, StandardCharsets.UTF_8));
  }

}
