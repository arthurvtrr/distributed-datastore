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
package io.atomix.catalyst.transport;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Transport server.
 * <p>
 * This is a low-level abstraction through which Catalyst servers receive connections from clients. Users should never use
 * this API directly.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Server {

  /**
   * Listens for connections on the server.
   * <p>
   * Once the server has started listening on the provided {@code address}, {@link Consumer#accept(Object)} will be
   * called for the provided {@link Consumer} each time a new connection to the server is established.
   * <p>
   * Once the server has bound to the provided {@link java.net.InetSocketAddress address} the returned
   * {@link java.util.concurrent.CompletableFuture} will be completed.
   *
   * @param address The address on which to listen for connections.
   * @return A completable future to be called once the server has started listening for connections.
   * @throws NullPointerException if {@code address} or {@code listener} are null
   */
  CompletableFuture<Void> listen(Address address, Consumer<Connection> listener);

  /**
   * Closes the server.
   * <p>
   * When the server is closed, any {@link Connection#onClose(Consumer) close listeners} registered
   * on the server's {@link Connection}s will be invoked prior to shutdown.
   *
   * @return A completable future to be completed once the server is closed.
   */
  CompletableFuture<Void> close();

}
