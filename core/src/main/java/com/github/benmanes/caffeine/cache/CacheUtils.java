/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.github.benmanes.caffeine.cache;

import com.github.benmanes.caffeine.cache.BoundedLocalCache.BoundedLocalManualCache;
import java.lang.reflect.Field;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheUtils {
  public static final Logger LOG = LoggerFactory.getLogger(CacheUtils.class);

  public static void closeScheduler(Cache<?, ?> cache)
      throws NoSuchFieldException, IllegalAccessException {
    Scheduler scheduler =
        ((GuardedScheduler) ((SSLA) ((BoundedLocalManualCache) cache).cache).pacer.scheduler)
            .delegate;
    ExecutorServiceScheduler serviceScheduler = (ExecutorServiceScheduler) scheduler;

    Field executorField =
        ExecutorServiceScheduler.class.getDeclaredField("scheduledExecutorService");
    executorField.setAccessible(true);
    ScheduledExecutorService executor =
        (ScheduledExecutorService) executorField.get(serviceScheduler);
    executor.shutdownNow();
  }
}
