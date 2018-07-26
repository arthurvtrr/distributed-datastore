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

import java.lang.ref.WeakReference;

/**
 * Catalyst thread.
 * <p>
 * The Catalyst thread primarily serves to store a {@link ThreadContext} for the current thread.
 * The context is stored in a {@link java.lang.ref.WeakReference} in order to allow the thread to be garbage collected.
 * <p>
 * There is no {@link ThreadContext} associated with the thread when it is first created.
 * It is the responsibility of thread creators to {@link #setContext(ThreadContext) set} the thread context when appropriate.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CatalystThread extends Thread {
  private WeakReference<ThreadContext> context;

  public CatalystThread(Runnable target, String name) {
    super(target, name);
  }

  /**
   * Sets the thread context.
   *
   * @param context The thread context.
   */
  public void setContext(ThreadContext context) {
    this.context = new WeakReference<>(context);
  }

  /**
   * Returns the thread context.
   *
   * @return The thread {@link ThreadContext} or {@code null} if no context has been configured.
   */
  public ThreadContext getContext() {
    return context != null ? context.get() : null;
  }

}
