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
package io.atomix.catalyst.transport.netty;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.buffer.Bytes;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Byte buffer output.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ByteBufOutput implements BufferOutput<ByteBufOutput> {
  ByteBuf buffer;

  /**
   * Sets the underlying byte buffer.
   */
  ByteBufOutput setByteBuf(ByteBuf buffer) {
    this.buffer = buffer;
    return this;
  }

  /**
   * Ensures that {@code size} bytes can be written to the buffer.
   */
  private void checkWrite(int size) {
    // If the buffer does not have enough bytes remaining, attempt to discard some of the read bytes.
    // It is possible that the buffer could discard 0 bytes, so we ensure the buffer is writable after
    // discarding some read bytes, and if not discard all read bytes.
    if (buffer.writerIndex() + size > buffer.maxCapacity()) {
      buffer.discardSomeReadBytes();
      if (buffer.writerIndex() + size > buffer.maxCapacity()) {
        buffer.discardReadBytes();
      }
    }
  }

  @Override
  public ByteBufOutput write(Buffer buffer) {
    int size = Math.min((int) buffer.remaining(), this.buffer.writableBytes());
    checkWrite(size);
    byte[] bytes = new byte[size];
    buffer.read(bytes);
    this.buffer.writeBytes(bytes);
    return this;
  }

  @Override
  public ByteBufOutput write(Bytes bytes) {
    int size = Math.min((int) bytes.size(), buffer.writableBytes());
    checkWrite(size);
    byte[] b = new byte[size];
    bytes.read(0, b, 0, b.length);
    buffer.writeBytes(b);
    return this;
  }

  @Override
  public ByteBufOutput write(byte[] bytes) {
    checkWrite(bytes.length);
    buffer.writeBytes(bytes);
    return this;
  }

  @Override
  public ByteBufOutput write(Bytes bytes, long offset, long length) {
    int size = Math.min((int) bytes.size(), (int) length);
    checkWrite(size);
    byte[] b = new byte[size];
    bytes.read(offset, b, 0, b.length);
    buffer.writeBytes(b);
    return this;
  }

  @Override
  public ByteBufOutput write(byte[] bytes, long offset, long length) {
    checkWrite((int) length);
    buffer.writeBytes(bytes, (int) offset, (int) length);
    return this;
  }

  @Override
  public ByteBufOutput writeByte(int b) {
    checkWrite(Bytes.BYTE);
    buffer.writeByte(b);
    return this;
  }

  @Override
  public ByteBufOutput writeUnsignedByte(int b) {
    checkWrite(Bytes.BYTE);
    buffer.writeByte(b);
    return this;
  }

  @Override
  public ByteBufOutput writeChar(char c) {
    checkWrite(Bytes.CHARACTER);
    buffer.writeChar(c);
    return this;
  }

  @Override
  public ByteBufOutput writeShort(short s) {
    checkWrite(Bytes.SHORT);
    buffer.writeShort(s);
    return this;
  }

  @Override
  public ByteBufOutput writeUnsignedShort(int s) {
    checkWrite(Bytes.SHORT);
    buffer.writeShort(s);
    return this;
  }

  @Override
  public ByteBufOutput writeInt(int i) {
    checkWrite(Bytes.INTEGER);
    buffer.writeInt(i);
    return this;
  }

  @Override
  public ByteBufOutput writeUnsignedInt(long i) {
    checkWrite(Bytes.INTEGER);
    buffer.writeInt((int) i);
    return this;
  }

  @Override
  public ByteBufOutput writeMedium(int m) {
    checkWrite(Bytes.MEDIUM);
    buffer.writeMedium(m);
    return this;
  }

  @Override
  public ByteBufOutput writeUnsignedMedium(int m) {
    checkWrite(Bytes.MEDIUM);
    buffer.writeMedium(m);
    return this;
  }

  @Override
  public ByteBufOutput writeLong(long l) {
    checkWrite(Bytes.LONG);
    buffer.writeLong(l);
    return this;
  }

  @Override
  public ByteBufOutput writeFloat(float f) {
    checkWrite(Bytes.FLOAT);
    buffer.writeFloat(f);
    return this;
  }

  @Override
  public ByteBufOutput writeDouble(double d) {
    checkWrite(Bytes.DOUBLE);
    buffer.writeDouble(d);
    return this;
  }

  @Override
  public ByteBufOutput writeBoolean(boolean b) {
    checkWrite(Bytes.BOOLEAN);
    buffer.writeBoolean(b);
    return this;
  }

  @Override
  public ByteBufOutput writeString(String s) {
    return writeString(s, Charset.defaultCharset());
  }

  @Override
  public ByteBufOutput writeString(String s, Charset charset) {
    if (s == null) {
      return writeBoolean(Boolean.FALSE);
    } else {
      byte[] bytes = s.getBytes(charset);
      writeBoolean(Boolean.TRUE);
      return writeUnsignedShort(bytes.length)
          .write(bytes, 0, bytes.length);
    }
  }

  @Override
  public ByteBufOutput writeUTF8(String s) {
    return writeString(s, StandardCharsets.UTF_8);
  }

  @Override
  public ByteBufOutput flush() {
    return this;
  }

  @Override
  public void close() {

  }

}
