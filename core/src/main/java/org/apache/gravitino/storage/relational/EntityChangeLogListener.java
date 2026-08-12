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

import java.util.List;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;

/** Listener for batches consumed from {@code entity_change_log}. */
@FunctionalInterface
public interface EntityChangeLogListener {

  /**
   * Handles a batch of entity changes.
   *
   * <p>The batch is dispatched exactly once and is never replayed, so implementations must be
   * self-healing: recover locally from a failure (for example by clearing the whole cache this
   * listener maintains, which is a superset of any invalidation it missed) rather than relying on
   * the poller to retry. If this method throws, the poller logs the failure at {@code ERROR} and
   * advances its cursor, which leaves the listener's state permanently stale.
   *
   * @param changes the entity changes fetched in one poller cycle
   */
  void onEntityChange(List<EntityChangeRecord> changes);
}
