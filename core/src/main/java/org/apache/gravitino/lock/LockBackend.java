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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;

/**
 * A pluggable backend that grants and releases hierarchical read/write locks keyed by {@link
 * NameIdentifier} paths.
 *
 * <p>Two implementations ship today: {@link InProcessLockBackend} (the default; preserves the
 * historical in-JVM {@link TreeLock} semantics) and {@link JdbcLockBackend} (cross-process
 * coordination using a relational database, intended for HA deployments). Additional backends
 * (ZooKeeper, etcd, Raft) can be added without touching the call sites in {@link TreeLockUtils}.
 *
 * <p>Implementations MUST acquire ancestor locks root-down to preserve the deadlock-free total
 * acquisition order on which existing call sites rely. See {@code design-docs/treelock-ha.md}.
 */
@Evolving
public interface LockBackend extends AutoCloseable {

  /**
   * Acquire a hierarchical lock on {@code identifier}. Ancestor nodes are taken with {@link
   * LockType#READ}; the leaf is taken with the requested {@code lockType}.
   *
   * @param identifier the resource path
   * @param lockType {@link LockType#READ} or {@link LockType#WRITE} for the leaf
   * @return an idempotent handle that releases all locks acquired by this call when closed
   * @throws RuntimeException if the lock cannot be acquired (timeout, backend unavailable, etc.).
   *     The exact subtype is backend-defined.
   */
  LockHandle acquire(NameIdentifier identifier, LockType lockType);

  /** Backend identifier for logs and metrics, e.g. {@code "inprocess"} or {@code "jdbc"}. */
  String name();

  /**
   * Release backend-owned resources (connection pools, schedulers, etc.). Idempotent.
   * Implementations that own no such resources may rely on the empty default.
   */
  @Override
  default void close() {}
}
