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
package org.apache.gravitino.storage.relational;

/**
 * Capability implemented by an {@link org.apache.gravitino.EntityStore} that maintains an {@code
 * entity_change_log} and runs a background poller dispatching consumed change batches to registered
 * listeners.
 *
 * <p>The poller is a side module owned and lifecycle-managed by the entity store itself (created
 * and started when the store is initialized, stopped when the store is closed). Consumers that need
 * to react to entity changes — e.g. for cross-node cache invalidation — register a listener through
 * this capability rather than reaching for a global component.
 */
public interface SupportsEntityChangeLog {

  /**
   * Registers a listener to receive future entity change batches.
   *
   * @param listener the listener to register
   */
  default void registerEntityChangeLogListener(EntityChangeLogListener listener) {
    // default no-op implementation since not all stores will support this capability
  }

  /**
   * Unregisters a previously registered listener.
   *
   * @param listener the listener to unregister
   */
  default void unregisterEntityChangeLogListener(EntityChangeLogListener listener) {
    // default no-op implementation since not all stores will support this capability
  }
}
