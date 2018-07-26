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
package io.atomix.catalyst.concurrent;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Thread context.
 * <p>
 * The thread context is used by Catalyst to determine the correct thread on which to execute asynchronous callbacks.
 * All threads created within Catalyst must be instances of {@link CatalystThread}. Once
 * a thread has been created, the context is stored in the thread object via
 * {@link CatalystThread#setContext(ThreadContext)}. This means there is a one-to-one relationship
 * between a context and a thread. That is, a context is representative of a thread and provides an interface for firing
 * events on that thread.
 * <p>
 * In addition to serving as an {@link java.util.concurrent.Executor}, the context also provides thread-local storage
 * for {@link Serializer} serializer instances. All serialization that takes place within a
 * {@link CatalystThread} should use the context {@link #serializer()}.
 * <p>
 * Components of the framework that provide custom threads should use {@link CatalystThreadFactory}
 * to allocate new threads and provide a custom {@link ThreadContext} implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ThreadContext extends AutoCloseable {

  /**
   * Returns the current thread context.
   *
   * @return The current thread context or {@code null} if no context exists.
   */
  static ThreadContext currentContext() {
    Thread thread = Thread.currentThread();
    return thread instanceof CatalystThread ? ((CatalystThread) thread).getContext() : null;
  }
  
  /**
   * @throws IllegalStateException if the current thread is not a catalyst thread
   */
  static ThreadContext currentContextOrThrow() {
    ThreadContext context = currentContext();
    Assert.state(context != null, "not on a Catalyst thread");
    return context;
  }

  /**
   * Returns a boolean indicating whether the current thread is in this context.
   *
   * @return Indicates whether the current thread is in this context.
   */
  default boolean isCurrentContext() {
    return currentContext() == this;
  }

  /**
   * Checks that the current thread is the correct context thread.
   */
  default void checkThread() {
    Assert.state(currentContext() == this, "not on a Catalyst thread");
  }

  /**
   * Returns the context logger.
   *
   * @return The context logger.
   */
  Logger logger();

  /**
   * Returns the context serializer.
   *
   * @return The context serializer.
   */
  Serializer serializer();

  /**
   * Returns the underlying executor.
   *
   * @return The underlying executor.
   */
  Executor executor();

  /**
   * Returns a boolean indicating whether the context state is blocked.
   *
   * @return Indicates whether the context state is blocked.
   */
  boolean isBlocked();

  /**
   * Sets the context state to blocked.
   */
  void block();

  /**
   * Sets the context state to unblocked.
   */
  void unblock();

  /**
   * Executes a callback on the context.
   *
   * @param callback The callback to execute.
   * @return A completable future to be completed once the callback has been executed.
   */
  default CompletableFuture<Void> execute(Runnable callback) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    executor().execute(() -> {
      try {
        callback.run();
        future.complete(null);
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    });
    return future;
  }

  /**
   * Executes a callback on the context.
   *
   * @param callback The callback to execute.
   * @param <T> The callback result type.
   * @return A completable future to be completed with the callback result.
   */
  default <T> CompletableFuture<T> execute(Supplier<T> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    executor().execute(() -> {
      try {
        future.complete(callback.get());
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    });
    return future;
  }

  /**
   * Schedules a runnable on the context.
   *
   * @param callback The callback to schedule.
   * @param delay The delay at which to schedule the runnable.
   */
  Scheduled schedule(Duration delay, Runnable callback);

  /**
   * Schedules a runnable at a fixed rate on the context.
   *
   * @param callback The callback to schedule.
   */
  Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback);

  /**
   * Closes the context.
   */
  @Override
  void close();

}
