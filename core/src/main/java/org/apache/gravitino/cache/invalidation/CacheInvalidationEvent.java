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

package org.apache.gravitino.cache.invalidation;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;

/** Immutable cache invalidation event. */
public final class CacheInvalidationEvent {
  private final CacheDomain domain;
  private final CacheInvalidationOperation operation;
  private final String sourceNode;
  private final long timestampMillis;
  @Nullable private final Object key;

  private CacheInvalidationEvent(
      CacheDomain domain,
      CacheInvalidationOperation operation,
      @Nullable Object key,
      String sourceNode,
      long timestampMillis) {
    this.domain = Preconditions.checkNotNull(domain, "domain cannot be null");
    this.operation = Preconditions.checkNotNull(operation, "operation cannot be null");
    this.sourceNode = Preconditions.checkNotNull(sourceNode, "sourceNode cannot be null");
    this.timestampMillis = timestampMillis;
    this.key = key;
  }

  public static CacheInvalidationEvent of(
      CacheDomain domain,
      CacheInvalidationOperation operation,
      @Nullable Object key,
      String sourceNode) {
    return new CacheInvalidationEvent(
        domain, operation, key, sourceNode, System.currentTimeMillis());
  }

  public CacheDomain domain() {
    return domain;
  }

  public CacheInvalidationOperation operation() {
    return operation;
  }

  @Nullable
  public Object key() {
    return key;
  }

  public String sourceNode() {
    return sourceNode;
  }

  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CacheInvalidationEvent)) {
      return false;
    }
    CacheInvalidationEvent that = (CacheInvalidationEvent) o;
    return timestampMillis == that.timestampMillis
        && domain == that.domain
        && operation == that.operation
        && Objects.equals(sourceNode, that.sourceNode)
        && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(domain, operation, sourceNode, timestampMillis, key);
  }
}
