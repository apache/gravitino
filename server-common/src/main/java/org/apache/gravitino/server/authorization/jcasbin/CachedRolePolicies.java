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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.Map;

/**
 * Cached, indexed privileges of a single role. The {@code updatedAt} timestamp corresponds to the
 * {@code role_meta.updated_at} column and is used as a version sentinel: if the DB value is newer,
 * the cached index is stale and must be rebuilt from the role entity.
 *
 * <p>The {@code index} maps each {@link PolicyKey} the role grants or denies to its {@link Effect},
 * with {@link Effect#DENY} already taking precedence over {@link Effect#ALLOW} within the role. A
 * privilege probe becomes a single {@link Map#get(Object)} instead of a linear scan over every
 * jcasbin policy line.
 */
final class CachedRolePolicies {

  private final long updatedAt;
  private final Map<PolicyKey, Effect> index;

  CachedRolePolicies(long updatedAt, Map<PolicyKey, Effect> index) {
    this.updatedAt = updatedAt;
    this.index = index;
  }

  long getUpdatedAt() {
    return updatedAt;
  }

  Map<PolicyKey, Effect> getIndex() {
    return index;
  }
}
