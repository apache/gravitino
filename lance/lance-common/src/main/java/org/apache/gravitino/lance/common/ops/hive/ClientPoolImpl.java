/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.common.ops.hive;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A simple connection pool implementation for reusing clients. Adapted from Apache Iceberg.
 *
 * @param <C> the client type
 * @param <E> the exception type thrown by client operations
 */
public abstract class ClientPoolImpl<C, E extends Exception> implements Closeable {

  private final int poolSize;
  private final Deque<C> clients;
  private final Class<? extends E> reconnectExc;
  private final boolean retryByDefault;
  private volatile int currentSize;
  private boolean closed;

  /**
   * Create a client pool.
   *
   * @param poolSize the maximum number of clients held by the pool
   * @param reconnectExc the exception type that triggers a reconnect when retry is enabled
   * @param retryByDefault whether {@link #run(Action)} retries on connection exceptions
   */
  protected ClientPoolImpl(int poolSize, Class<? extends E> reconnectExc, boolean retryByDefault) {
    this.poolSize = poolSize;
    this.clients = new ArrayDeque<>();
    this.reconnectExc = reconnectExc;
    this.retryByDefault = retryByDefault;
    this.currentSize = 0;
    this.closed = false;
  }

  /**
   * An action executed against a pooled client.
   *
   * @param <R> the result type
   * @param <C> the client type
   * @param <E> the exception type that may be thrown
   */
  public interface Action<R, C, E extends Exception> {
    /**
     * Run the action against the given client.
     *
     * @param client the pooled client
     * @return the action result
     * @throws E if the action fails
     */
    R run(C client) throws E;
  }

  /**
   * Run an action against a pooled client using the default retry policy.
   *
   * @param action the action to run
   * @param <R> the result type
   * @return the action result
   * @throws E if the action fails
   * @throws InterruptedException if interrupted while waiting for a client
   */
  public <R> R run(Action<R, C, E> action) throws E, InterruptedException {
    return run(action, retryByDefault);
  }

  /**
   * Run an action against a pooled client.
   *
   * @param action the action to run
   * @param retry whether to retry once on a connection exception
   * @param <R> the result type
   * @return the action result
   * @throws E if the action fails
   * @throws InterruptedException if interrupted while waiting for a client
   */
  @SuppressWarnings("unchecked")
  public <R> R run(Action<R, C, E> action, boolean retry) throws E, InterruptedException {
    C client = get();
    try {
      return action.run(client);
    } catch (Exception exc) {
      if (retry && isConnectionException(exc)) {
        try {
          client = reconnect(client);
        } catch (Exception reconnectExc) {
          release(client);
          throw (E) exc;
        }
        return action.run(client);
      }
      throw (E) exc;
    } finally {
      release(client);
    }
  }

  /**
   * Create a new client.
   *
   * @return a newly created client
   */
  protected abstract C newClient();

  /**
   * Reconnect a client that hit a connection exception.
   *
   * @param client the client to reconnect
   * @return the reconnected client
   */
  protected abstract C reconnect(C client);

  /**
   * Close a client.
   *
   * @param client the client to close
   */
  protected abstract void close(C client);

  /**
   * Whether the given exception indicates a connection problem.
   *
   * @param exc the exception to inspect
   * @return true if the exception is a connection exception
   */
  protected boolean isConnectionException(Exception exc) {
    return reconnectExc.isInstance(exc);
  }

  private synchronized C get() throws InterruptedException {
    if (closed) {
      throw new IllegalStateException("Cannot get a client from a closed pool");
    }

    while (clients.isEmpty() && currentSize >= poolSize) {
      wait();
    }

    if (!clients.isEmpty()) {
      return clients.removeFirst();
    }

    currentSize++;
    return newClient();
  }

  private synchronized void release(C client) {
    if (closed) {
      close(client);
    } else {
      clients.addFirst(client);
      notify();
    }
  }

  @Override
  public synchronized void close() {
    this.closed = true;
    while (!clients.isEmpty()) {
      close(clients.removeFirst());
    }
    notifyAll();
  }
}
