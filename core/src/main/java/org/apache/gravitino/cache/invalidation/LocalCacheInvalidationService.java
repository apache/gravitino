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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** In-JVM cache invalidation service implementation. */
public class LocalCacheInvalidationService implements CacheInvalidationService {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheInvalidationService.class);

  private final boolean enabled;
  private final String localNodeId;
  private final Map<CacheDomain, ConcurrentMap<String, CacheInvalidationHandler>> handlers;
  private final ConcurrentMap<String, AtomicLong> publishedCounters;
  private final ConcurrentMap<String, AtomicLong> handledCounters;

  public LocalCacheInvalidationService(boolean enabled) {
    this.enabled = enabled;
    this.localNodeId = buildLocalNodeId();
    this.handlers = new EnumMap<>(CacheDomain.class);
    for (CacheDomain domain : CacheDomain.values()) {
      handlers.put(domain, new ConcurrentHashMap<>());
    }
    this.publishedCounters = new ConcurrentHashMap<>();
    this.handledCounters = new ConcurrentHashMap<>();
  }

  @Override
  public void publish(CacheInvalidationEvent event) {
    Preconditions.checkNotNull(event, "event cannot be null");
    if (!enabled) {
      return;
    }

    String counterKey = getCounterKey(event.domain(), event.operation());
    publishedCounters.computeIfAbsent(counterKey, key -> new AtomicLong()).incrementAndGet();

    ConcurrentMap<String, CacheInvalidationHandler> domainHandlers = handlers.get(event.domain());
    if (domainHandlers.isEmpty()) {
      LOG.warn("No cache invalidation handlers registered for domain {}", event.domain());
      return;
    }

    domainHandlers.forEach(
        (handlerId, handler) -> {
          handler.handle(event);
          handledCounters.computeIfAbsent(counterKey, key -> new AtomicLong()).incrementAndGet();
        });
  }

  @Override
  public void registerHandler(
      CacheDomain domain, String handlerId, CacheInvalidationHandler handler) {
    Preconditions.checkNotNull(domain, "domain cannot be null");
    Preconditions.checkArgument(handlerId != null && !handlerId.isBlank(), "handlerId is required");
    Preconditions.checkNotNull(handler, "handler cannot be null");
    handlers.get(domain).put(handlerId, handler);
  }

  @Override
  public void unregisterHandler(CacheDomain domain, String handlerId) {
    Preconditions.checkNotNull(domain, "domain cannot be null");
    if (handlerId == null || handlerId.isBlank()) {
      return;
    }
    handlers.get(domain).remove(handlerId);
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public String localNodeId() {
    return localNodeId;
  }

  @VisibleForTesting
  long getPublishedCount(CacheDomain domain, CacheInvalidationOperation operation) {
    return publishedCounters.getOrDefault(getCounterKey(domain, operation), new AtomicLong()).get();
  }

  @VisibleForTesting
  long getHandledCount(CacheDomain domain, CacheInvalidationOperation operation) {
    return handledCounters.getOrDefault(getCounterKey(domain, operation), new AtomicLong()).get();
  }

  private String getCounterKey(CacheDomain domain, CacheInvalidationOperation operation) {
    return String.format("%s:%s", domain.name(), operation.name());
  }

  private String buildLocalNodeId() {
    String host = "unknown-host";
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOG.warn("Failed to resolve local host name for cache invalidation node id", e);
    }
    return host + "-" + ManagementFactory.getRuntimeMXBean().getName();
  }
}
