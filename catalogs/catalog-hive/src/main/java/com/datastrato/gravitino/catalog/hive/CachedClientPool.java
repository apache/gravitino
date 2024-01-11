/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.utils.ClientPool;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.immutables.value.Value;

/**
 * Referred from Apache Iceberg's CachedClientPool implementation
 * hive-metastore/src/main/java/org/apache/iceberg/hive/CachedClientPool.java
 *
 * A ClientPool that caches the underlying HiveClientPool instances.
 */
public class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

  private static Cache<Key, HiveClientPool> clientPoolCache;

  private final Configuration conf;
  private final int clientPoolSize;
  private final long evictionInterval;
  private final String cacheKeys;

  CachedClientPool(
      int clientPoolSize, Configuration conf, long evictionInterval, String cacheKeys) {
    this.conf = conf;
    this.clientPoolSize = clientPoolSize;
    this.evictionInterval = evictionInterval;
    this.cacheKeys = cacheKeys;
    init();
  }

  @VisibleForTesting
  HiveClientPool clientPool() {
    Key key = extractKey(clientPoolSize, cacheKeys, conf);
    return clientPoolCache.get(key, k -> new HiveClientPool(clientPoolSize, conf));
  }

  private synchronized void init() {
    if (clientPoolCache == null) {
      // Since Caffeine does not ensure that removalListener will be involved after expiration
      // We use a scheduler with one thread to clean up expired clients.
      clientPoolCache =
          Caffeine.newBuilder()
              .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
              .removalListener((ignored, value, cause) -> ((HiveClientPool) value).close())
              .scheduler(
                  Scheduler.forScheduledExecutorService(
                      new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory())))
              .build();
    }
  }

  @VisibleForTesting
  static Cache<Key, HiveClientPool> clientPoolCache() {
    return clientPoolCache;
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action)
      throws TException, InterruptedException {
    return clientPool().run(action);
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
      throws TException, InterruptedException {
    return clientPool().run(action, retry);
  }

  @VisibleForTesting
  static Key extractKey(int clientPoolSize, String cacheKeys, Configuration conf) {
    // generate key elements in a certain order, so that the Key instances are comparable
    List<Object> elements = Lists.newArrayList();
    elements.add(conf.get(HiveConf.ConfVars.METASTOREURIS.varname, ""));
    elements.add(clientPoolSize);
    try {
      elements.add(UserGroupInformation.getCurrentUser().getUserName());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    if (cacheKeys == null || cacheKeys.isEmpty()) {
      return Key.of(elements);
    }

    Map<String, String> confElements = Maps.newTreeMap();
    for (String element : cacheKeys.split(",", -1)) {
      String trimmed = element.trim();
        Preconditions.checkArgument(
            !confElements.containsKey(trimmed), "Conf key element %s already specified", trimmed);
        confElements.put(trimmed, conf.get(trimmed));
    }

    for (String key : confElements.keySet()) {
      elements.add(ConfElement.of(key, confElements.get(key)));
    }
    return Key.of(elements);
  }

  @Value.Immutable
  abstract static class Key {

    abstract List<Object> elements();

    private static Key of(Iterable<?> elements) {
      return ImmutableKey.builder().elements(elements).build();
    }
  }

  @Value.Immutable
  abstract static class ConfElement {
    abstract String key();

    @Nullable
    abstract String value();

    static ConfElement of(String key, String value) {
      return ImmutableConfElement.builder().key(key).value(value).build();
    }
  }

  private static ThreadFactory newDaemonThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("hive-metastore-cleaner" + "-%d")
        .build();
  }
}
