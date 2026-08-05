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

package org.apache.gravitino.cache;

/**
 * Describes how an {@link EntityCache} implementation stays coherent across a multi-node Gravitino
 * cluster. The write path reads this marker to decide whether a change made on one node must be
 * propagated to the other nodes.
 */
public enum Coherence {
  /**
   * Each node keeps its own copy of the cache, so a change made on one node does not clear the
   * copies held by the other nodes. To stay correct across a cluster the write path must publish
   * the change (via the {@code entity_change_log}) so every other node's poller invalidates its own
   * copy. The default {@code caffeine} cache is {@code LOCAL_PER_NODE}.
   */
  LOCAL_PER_NODE,

  /**
   * A single copy is shared by the whole cluster, so a write clears it once and every node observes
   * the change immediately. There is nothing per-node to propagate. The {@code redis} cache is
   * {@code SHARED}.
   */
  SHARED
}
