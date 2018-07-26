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
package io.atomix.catalyst.buffer;

import io.atomix.catalyst.buffer.util.Memory;
import io.atomix.catalyst.util.reference.ReferenceManager;

import java.io.File;
import java.nio.channels.FileChannel;

/**
 * File buffer.
 * <p>
 * File buffers wrap a simple {@link java.io.RandomAccessFile} instance to provide random access to a file on local disk. All
 * operations are delegated directly to the {@link java.io.RandomAccessFile} interface, and limitations are dependent on the
 * semantics of the underlying file.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileBuffer extends AbstractBuffer {

  /**
   * Allocates a file buffer of unlimited capacity.
   * <p>
   * The buffer will initially be allocated with {@code 4096} bytes. As bytes are written to the resulting buffer and
   * the original capacity is reached, the buffer's capacity will double.
   *
   * @param file The file to allocate.
   * @return The allocated buffer.
   *
   * @see FileBuffer#allocate(java.io.File, long)
   * @see FileBuffer#allocate(java.io.File, long, long)
   * @see FileBuffer#allocate(java.io.File, String, long, long)
   */
  public static FileBuffer allocate(File file) {
    return allocate(file, FileBytes.DEFAULT_MODE, DEFAULT_INITIAL_CAPACITY, Long.MAX_VALUE);
  }

  /**
   * Allocates a file buffer with the given initial capacity.
   * <p>
   * If the underlying file is empty, the file count will expand dynamically as bytes are written to the file.
   * The underlying {@link FileBytes} will be initialized to the nearest power of {@code 2}.
   *
   * @param file The file to allocate.
   * @param initialCapacity The initial capacity of the bytes to allocate.
   * @return The allocated buffer.
   *
   * @see FileBuffer#allocate(java.io.File)
   * @see FileBuffer#allocate(java.io.File, long, long)
   * @see FileBuffer#allocate(java.io.File, String, long, long)
   */
  public static FileBuffer allocate(File file, long initialCapacity) {
    return allocate(file, FileBytes.DEFAULT_MODE, initialCapacity, Long.MAX_VALUE);
  }

  /**
   * Allocates a file buffer.
   * <p>
   * The underlying {@link java.io.RandomAccessFile} will be created in {@code rw} mode by default.
   * The resulting buffer will be initialized with a capacity of {@code initialCapacity}. The underlying {@link FileBytes}
   * will be initialized to the nearest power of {@code 2}. As bytes are written to the file the buffer's capacity will
   * double up to {@code maxCapacity}.
   *
   * @param file The file to allocate.
   * @param initialCapacity The initial capacity of the buffer.
   * @param maxCapacity The maximum allowed capacity of the buffer.
   * @return The allocated buffer.
   *
   * @see FileBuffer#allocate(java.io.File)
   * @see FileBuffer#allocate(java.io.File, long)
   * @see FileBuffer#allocate(java.io.File, String, long, long)
   */
  public static FileBuffer allocate(File file, long initialCapacity, long maxCapacity) {
    return allocate(file, FileBytes.DEFAULT_MODE, initialCapacity, maxCapacity);
  }

  /**
   * Allocates a file buffer.
   * <p>
   * The resulting buffer will be initialized with a capacity of {@code initialCapacity}. The underlying {@link FileBytes}
   * will be initialized to the nearest power of {@code 2}. As bytes are written to the file the buffer's capacity will
   * double up to {@code maxCapacity}.
   *
   * @param file The file to allocate.
   * @param mode The mode in which to open the underlying {@link java.io.RandomAccessFile}.
   * @param initialCapacity The initial capacity of the buffer.
   * @param maxCapacity The maximum allowed capacity of the buffer.
   * @return The allocated buffer.
   *
   * @see FileBuffer#allocate(java.io.File)
   * @see FileBuffer#allocate(java.io.File, long)
   * @see FileBuffer#allocate(java.io.File, long, long)
   */
  public static FileBuffer allocate(File file, String mode, long initialCapacity, long maxCapacity) {
    return new FileBuffer(new FileBytes(file, mode, Memory.Util.toPow2(initialCapacity)), 0, initialCapacity, maxCapacity);
  }

  private FileBuffer(Bytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
  }

  private FileBuffer(Bytes bytes, long offset, long initialCapacity, long maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity, null);
  }

  /**
   * Returns the underlying file object.
   *
   * @return The underlying file.
   */
  public File file() {
    return ((FileBytes) bytes).file();
  }

  /**
   * Maps a portion of the underlying file into memory in {@link java.nio.channels.FileChannel.MapMode#READ_WRITE} mode
   * starting at the current position up to the given {@code count}.
   *
   * @param size The count of the bytes to map into memory.
   * @return The mapped buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed
   *         {@link java.nio.MappedByteBuffer} count: {@link Integer#MAX_VALUE}
   */
  public UnsafeMappedBuffer map(long size) {
    return map(position(), size, FileChannel.MapMode.READ_WRITE);
  }

  /**
   * Maps a portion of the underlying file into memory starting at the current position up to the given {@code count}.
   *
   * @param size The count of the bytes to map into memory.
   * @param mode The mode in which to map the bytes into memory.
   * @return The mapped buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed
   *         {@link java.nio.MappedByteBuffer} count: {@link Integer#MAX_VALUE}
   */
  public UnsafeMappedBuffer map(long size, FileChannel.MapMode mode) {
    return map(position(), size, mode);
  }

  /**
   * Maps a portion of the underlying file into memory in {@link java.nio.channels.FileChannel.MapMode#READ_WRITE} mode
   * starting at the given {@code offset} up to the given {@code count}.
   *
   * @param offset The offset from which to map bytes into memory.
   * @param size The count of the bytes to map into memory.
   * @return The mapped buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed
   *         {@link java.nio.MappedByteBuffer} count: {@link Integer#MAX_VALUE}
   */
  public UnsafeMappedBuffer map(long offset, long size) {
    return map(offset, size, FileChannel.MapMode.READ_WRITE);
  }

  /**
   * Maps a portion of the underlying file into memory starting at the given {@code offset} up to the given {@code count}.
   *
   * @param offset The offset from which to map bytes into memory.
   * @param size The count of the bytes to map into memory.
   * @param mode The mode in which to map the bytes into memory.
   * @return The mapped buffer.
   * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed
   *         {@link java.nio.MappedByteBuffer} count: {@link Integer#MAX_VALUE}
   */
  public UnsafeMappedBuffer map(long offset, long size, FileChannel.MapMode mode) {
    return new UnsafeMappedBuffer(((FileBytes) bytes).map(offset, size, mode), 0, size, size);
  }

  @Override
  protected void compact(long from, long to, long length) {
    byte[] bytes = new byte[1024];
    long position = from;
    while (position < from + length) {
      long size = Math.min((from + length) - position, 1024);
      this.bytes.read(position, bytes, 0, size);
      this.bytes.write(0, bytes, 0, size);
      position += size;
    }
  }

  /**
   * Deletes the underlying file.
   */
  public void delete() {
    ((FileBytes) bytes).delete();
  }

}
