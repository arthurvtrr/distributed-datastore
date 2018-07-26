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

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Calendar serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CalendarSerializer implements TypeSerializer<Calendar> {

  @Override
  public void write(Calendar calendar, BufferOutput buffer, Serializer serializer) {
    buffer.writeUTF8(calendar.getTimeZone().getID());
    buffer.writeLong(calendar.getTimeInMillis());
  }

  @Override
  public Calendar read(Class<Calendar> type, BufferInput buffer, Serializer serializer) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(buffer.readUTF8()));
    calendar.setTimeInMillis(buffer.readLong());
    return calendar;
  }

}
