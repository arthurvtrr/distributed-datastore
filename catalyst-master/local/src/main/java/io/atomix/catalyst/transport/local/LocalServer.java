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
package io.atomix.catalyst.transport.local;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.concurrent.ThreadContext;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Local server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalServer implements Server {
  private final UUID id = UUID.randomUUID();
  private final LocalServerRegistry registry;
  private final Set<LocalConnection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private volatile Address address;
  private volatile ListenerHolder listener;

  /**
   * @throws NullPointerException if any argument is null
   */
  public LocalServer(LocalServerRegistry registry) {
    this.registry = Assert.notNull(registry, "registry");
  }

  /**
   * Connects to the server.
   */
  CompletableFuture<Void> connect(LocalConnection connection) {
    LocalConnection localConnection = new LocalConnection(listener.context, connections);
    connections.add(localConnection);
    connection.connect(localConnection);
    localConnection.connect(connection);
    return CompletableFuture.runAsync(() -> listener.listener.accept(localConnection), listener.context.executor());
  }

  @Override
  public synchronized CompletableFuture<Void> listen(Address address, Consumer<Connection> listener) {
    Assert.notNull(address, "address");
    Assert.notNull(listener, "listener");
    if (this.address != null) {
      if (!this.address.equals(address)) {
        throw new IllegalStateException(String.format("already listening at %s", this.address));
      }
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    registry.register(address, this);
    ThreadContext context = ThreadContext.currentContextOrThrow();

    this.address = address;
    this.listener = new ListenerHolder(listener, context);

    context.execute(() -> future.complete(null));
    return future;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (address == null)
      return CompletableFuture.completedFuture(null);

    CompletableFuture<Void> future = new CompletableFuture<>();
    registry.unregister(address);
    address = null;
    listener = null;

    ThreadContext context = ThreadContext.currentContextOrThrow();
    CompletableFuture<?>[] futures = new CompletableFuture[connections.size()];
    int i = 0;
    for (LocalConnection connection : connections) {
      futures[i++] = connection.close();
    }
    CompletableFuture.allOf(futures).thenRunAsync(() -> future.complete(null), context.executor());
    return future;
  }

  /**
   * Listener holder.
   */
  private static class ListenerHolder {
    private final Consumer<Connection> listener;
    private final ThreadContext context;

    private ListenerHolder(Consumer<Connection> listener, ThreadContext context) {
      this.listener = listener;
      this.context = context;
    }
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof LocalServer && ((LocalServer) object).id.equals(id);
  }

}
