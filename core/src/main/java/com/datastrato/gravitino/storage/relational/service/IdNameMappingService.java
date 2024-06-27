/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.NameIdentifier;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class IdNameMappingService implements Closeable {

  private static volatile IdNameMappingService instance;

  private Cache<NameIdentifier, Long> ident2IdCache;

  private IdNameMappingService() {
    this.ident2IdCache =
        Caffeine.newBuilder()
            .expireAfterAccess(24 * 3600 * 1000 /* 1 day */, TimeUnit.MILLISECONDS)
            .maximumSize(1000000)
            .initialCapacity(1000)
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("ident-to-id-cleaner-%d")
                            .build())))
            .build();
  }

  public static IdNameMappingService getInstance() {
    if (instance == null) {
      synchronized (IdNameMappingService.class) {
        if (instance == null) {
          instance = new IdNameMappingService();
        }
      }
    }

    return instance;
  }

  public void put(NameIdentifier key, Long value) {
    ident2IdCache.put(key, value);
  }

  public Long get(NameIdentifier key, Function<NameIdentifier, Long> mappingFunction) {
    return ident2IdCache.get(key, mappingFunction);
  }

  public void invalidate(NameIdentifier key) {
    ident2IdCache.invalidate(key);
  }

  public void invalidateWithPrefix(NameIdentifier nameIdentifier) {
    ident2IdCache.asMap().keySet().stream()
        .filter(k -> k.toString().startsWith(nameIdentifier.toString()))
        .forEach(ident2IdCache::invalidate);
  }

  @Override
  public void close() throws IOException {
    if (ident2IdCache != null) {
      ident2IdCache.invalidateAll();
      ident2IdCache.cleanUp();
    }
  }
}
