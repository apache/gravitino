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

package org.apache.gravitino.lock;

import org.apache.gravitino.annotation.Evolving;

/**
 * A handle representing a successfully acquired lock from a {@link LockBackend}. {@link #close()}
 * releases every lock acquired by the originating {@code acquire(...)} call, in reverse acquisition
 * order, and must be idempotent.
 *
 * <p>Intended to be used with try-with-resources:
 *
 * <pre>{@code
 * try (LockHandle handle = backend.acquire(ident, LockType.WRITE)) {
 *   // operation runs while the lock is held
 * }
 * }</pre>
 */
@Evolving
public interface LockHandle extends AutoCloseable {

  /**
   * Release all locks held by this handle. Idempotent — calling {@code close()} more than once is a
   * no-op. Must not throw checked exceptions.
   */
  @Override
  void close();
}
