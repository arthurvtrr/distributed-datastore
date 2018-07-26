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
 * Double array serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DoubleArraySerializer implements TypeSerializer<double[]> {

  @Override
  public void write(double[] doubles, BufferOutput buffer, Serializer serializer) {
    buffer.writeUnsignedShort(doubles.length);
    for (double d : doubles) {
      buffer.writeDouble(d);
    }
  }

  @Override
  public double[] read(Class<double[]> type, BufferInput buffer, Serializer serializer) {
    double[] doubles = new double[buffer.readUnsignedShort()];
    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = buffer.readDouble();
    }
    return doubles;
  }

}
