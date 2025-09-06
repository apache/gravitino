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
 *
 */
package org.apache.gravitino.iceberg.common.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Referred from Apache Iceberg's CachedClientPool implementation
 * hive-metastore/src/main/java/org/apache/iceberg/hive/CachedClientPool.java
 *
 * <p>IcebergHiveCachedClientPool is used for every Iceberg catalog with Hive backend, I changed the
 * method clientPool() from
 *
 * <pre>{@code
 * HiveClientPool clientPool() {
 *    return clientPoolCache.get(key, k -> new HiveClientPool(clientPoolSize, conf));
 *  }
 * }</pre>
 *
 * to
 *
 * <pre>{@code
 * HiveClientPool clientPool() {
 *   Key key = extractKey(properties.get(CatalogProperties.CLIENT_POOL_CACHE_KEYS), conf);
 *   return clientPoolCache.get(key, k -> new HiveClientPool(clientPoolSize, conf));
 * }
 * }</pre>
 *
 * Why do we need to do this? Because the original client pool in Apache Iceberg uses a fixed
 * username to create the client pool (please see the key in the method clientPool()). Assuming the
 * original name is A and when a new user B tries to call the clientPool() method, it will use the
 * connection that belongs to A. This will not work with kerberos authentication as it will change
 * the user name.
 */
public class IcebergHiveCachedClientPool
    implements ClientPool<IMetaStoreClient, TException>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergHiveCachedClientPool.class);

  private static final String CONF_ELEMENT_PREFIX = "conf:";

  private static Cache<Key, HiveClientPool> clientPoolCache;

  private final Configuration conf;
  private final Map<String, String> properties;
  private final int clientPoolSize;
  private final long evictionInterval;
  private ScheduledExecutorService scheduledExecutorService;

  public IcebergHiveCachedClientPool(Configuration conf, Map<String, String> properties) {
    this.conf = conf;
    this.clientPoolSize =
        PropertyUtil.propertyAsInt(
            properties,
            CatalogProperties.CLIENT_POOL_SIZE,
            CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
    this.evictionInterval =
        PropertyUtil.propertyAsLong(
            properties,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
    this.properties = properties;
    init();
  }

  @VisibleForTesting
  HiveClientPool clientPool() {
    Key key = extractKey(properties.get(CatalogProperties.CLIENT_POOL_CACHE_KEYS), conf);
    return clientPoolCache.get(
        key,
        k -> {
          HiveClientPool hiveClientPool = new HiveClientPool(clientPoolSize, conf);
          LOG.info("Created a new HiveClientPool instance: {} for Key: {}", hiveClientPool, key);
          return hiveClientPool;
        });
  }

  private synchronized void init() {
    if (clientPoolCache == null) {
      // Since Caffeine does not ensure that removalListener will be involved after expiration
      // We use a scheduler with one thread to clean up expired clients.
      scheduledExecutorService = ThreadPools.newScheduledPool("hive-metastore-cleaner", 1);
      clientPoolCache =
          Caffeine.newBuilder()
              .expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
              .removalListener(
                  (key, value, cause) -> {
                    HiveClientPool hiveClientPool = (HiveClientPool) value;
                    if (hiveClientPool != null) {
                      LOG.info(
                          "Removing an expired HiveClientPool instance: {} for Key: {}",
                          hiveClientPool,
                          key);
                      hiveClientPool.close();
                    }
                  })
              .scheduler(Scheduler.forScheduledExecutorService(scheduledExecutorService))
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
  static Key extractKey(String cacheKeys, Configuration conf) {
    // generate key elements in a certain order, so that the Key instances are comparable
    List<Object> elements = Lists.newArrayList();
    elements.add(conf.get(HiveConf.ConfVars.METASTOREURIS.varname, ""));
    elements.add(conf.get("HIVE_CONF_CATALOG", "hive"));
    if (cacheKeys == null || cacheKeys.isEmpty()) {
      return Key.of(elements);
    }

    Set<KeyElementType> types = Sets.newTreeSet(Comparator.comparingInt(Enum::ordinal));
    Map<String, String> confElements = Maps.newTreeMap();
    for (String element : cacheKeys.split(",", -1)) {
      String trimmed = element.trim();
      if (trimmed.toLowerCase(Locale.ROOT).startsWith(CONF_ELEMENT_PREFIX)) {
        String key = trimmed.substring(CONF_ELEMENT_PREFIX.length());
        ValidationException.check(
            !confElements.containsKey(key), "Conf key element %s already specified", key);
        confElements.put(key, conf.get(key));
      } else {
        KeyElementType type = KeyElementType.valueOf(trimmed.toUpperCase());
        switch (type) {
          case UGI:
          case USER_NAME:
            ValidationException.check(
                !types.contains(type), "%s key element already specified", type.name());
            types.add(type);
            break;
          default:
            throw new ValidationException("Unknown key element %s", trimmed);
        }
      }
    }
    for (KeyElementType type : types) {
      switch (type) {
        case UGI:
          try {
            elements.add(UserGroupInformation.getCurrentUser());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          break;
        case USER_NAME:
          try {
            elements.add(UserGroupInformation.getCurrentUser().getUserName());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          break;
        default:
          throw new RuntimeException("Unexpected key element " + type.name());
      }
    }
    return Key.of(elements);
  }

  static class Key {
    private final List<Object> elements;

    List<Object> elements() {
      return elements;
    }

    public Key(List<Object> elements) {
      this.elements = elements;
    }

    static Key of(List<Object> elements) {
      return new Key(elements);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key key = (Key) o;
      return Objects.equals(elements, key.elements);
    }

    @Override
    public int hashCode() {
      return Objects.hash(elements);
    }
  }

  private enum KeyElementType {
    UGI,
    USER_NAME,
    CONF
  }

  @Override
  public void close() throws IOException {
    clientPoolCache.asMap().forEach((key, value) -> value.close());
    clientPoolCache.invalidateAll();
    if (scheduledExecutorService != null) scheduledExecutorService.shutdownNow();
  }
}
