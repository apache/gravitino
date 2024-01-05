/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergRestUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.MetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergMetricsManager {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableOps.class);
  public static final String ICEBERG_METRICS_STORE = "metricsStore";
  public static final String ICEBERG_METRICS_STORE_RETAIN_DAYS = "metricsStoreRetainDays";
  public static final String ICEBERG_METRICS_QUEUE_CAPACITY = "metricsQueueCapacity";

  // Register IcebergMetricsStore's short name to its full qualified class name in the map. So
  // that user doesn't need to specify the full qualified class name when creating an
  // IcebergMetricsStore.
  private static final ImmutableMap<String, String> ICEBERG_METRICS_STORE_NAMES =
      ImmutableMap.of(
          DummyMetricsStore.ICEBERG_METRICS_STORE_DUMMY_NAME,
          DummyMetricsStore.class.getCanonicalName());

  private final IcebergMetricsFormatter icebergMetricsFormatter;
  private final IcebergMetricsStore icebergMetricsStore;
  private final int retainDays;

  private BlockingQueue<MetricsReport> queue;
  private Thread metricsWriterThread;
  private volatile boolean isClosed = false;
  private Optional<ScheduledExecutorService> metricsCleanerExecutor = Optional.empty();

  public IcebergMetricsManager(IcebergConfig icebergConfig) {
    icebergMetricsFormatter = new IcebergMetricsFormatter();
    icebergMetricsStore =
        loadIcebergMetricsStore(icebergConfig.get(IcebergConfig.ICEBERG_METRICS_STORE));
    try {
      icebergMetricsStore.init(icebergConfig.getAllConfig());
    } catch (IOException e) {
      LOG.warn("Iceberg metrics store init failed.", e);
      throw new RuntimeException(e);
    }

    retainDays = icebergConfig.get(IcebergConfig.ICEBERG_METRICS_STORE_RETAIN_DAYS);
    if (retainDays > 0) {
      metricsCleanerExecutor =
          Optional.of(
              new ScheduledThreadPoolExecutor(
                  1,
                  new ThreadFactoryBuilder()
                      .setDaemon(true)
                      .setNameFormat("Iceberg-metrics-cleaner")
                      .setUncaughtExceptionHandler(
                          (t, e) -> LOG.error("Uncaught exception in thread {}.", t, e))
                      .build()));
    }

    int queueCapacity = icebergConfig.get(IcebergConfig.ICEBERG_METRICS_QUEUE_CAPACITY);
    queue = new LinkedBlockingQueue(queueCapacity);
    metricsWriterThread = new Thread(() -> writeMetrics());
    metricsWriterThread.setName("Iceberg-metrics-writer");
    metricsWriterThread.setDaemon(true);
  }

  public void start() {
    metricsWriterThread.start();
    metricsCleanerExecutor.ifPresent(
        executorService ->
            executorService.scheduleAtFixedRate(
                () -> {
                  Instant now = Instant.now();
                  Instant expireTime =
                      IcebergRestUtils.calculateNewTimestamp(now, -24 * retainDays);
                  LOG.info("Try clean Iceberg expired metrics, {}.", expireTime);
                  try {
                    icebergMetricsStore.clean(expireTime);
                  } catch (Exception e) {
                    LOG.warn("Clean Iceberg metrics failed.", e);
                  }
                },
                0,
                1,
                TimeUnit.HOURS));
  }

  public void recordMetric(MetricsReport metricsReport) {
    if (isClosed) {
      logMetrics("Drop Iceberg metrics because Iceberg Metrics Manager is closed.", metricsReport);
      return;
    }
    if (queue.offer(metricsReport) == false) {
      logMetrics("Drop Iceberg metrics because metrics queue is full.", metricsReport);
    }
  }

  public void close() {
    isClosed = true;
    metricsCleanerExecutor.ifPresent(executorService -> executorService.shutdownNow());

    if (icebergMetricsStore != null) {
      try {
        icebergMetricsStore.close();
      } catch (IOException e) {
        LOG.warn("Close Iceberg metrics store failed.", e);
      }
    }

    if (metricsWriterThread != null) {
      metricsWriterThread.interrupt();
      try {
        metricsWriterThread.join();
      } catch (InterruptedException e) {
        LOG.warn("Iceberg metrics manager is interrupted while join metrics writer thread.");
      }
    }
  }

  @VisibleForTesting
  IcebergMetricsStore getIcebergMetricsStore() {
    return icebergMetricsStore;
  }

  private void writeMetrics() {
    while (Thread.currentThread().isInterrupted() == false) {
      MetricsReport metricsReport;
      try {
        metricsReport = queue.take();
      } catch (InterruptedException e) {
        LOG.warn("Iceberg Metrics writer thread is interrupted.");
        break;
      }
      if (metricsReport != null) {
        doRecordMetric(metricsReport);
      }
    }

    MetricsReport metricsReport = queue.poll();
    while (metricsReport != null) {
      logMetrics("Drop Iceberg metrics because it's time to close metrics store.", metricsReport);
      metricsReport = queue.poll();
    }
  }

  private IcebergMetricsStore loadIcebergMetricsStore(String metricsStoreName) {
    if (metricsStoreName == null) {
      metricsStoreName = DummyMetricsStore.ICEBERG_METRICS_STORE_DUMMY_NAME;
    }
    String metricsStoreClass =
        ICEBERG_METRICS_STORE_NAMES.getOrDefault(metricsStoreName, metricsStoreName);
    LOG.info("Load Iceberg metrics store: {}.", metricsStoreClass);
    try {
      return (IcebergMetricsStore)
          Class.forName(metricsStoreClass).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error(
          "Failed to create and initialize Iceberg metrics store by name {}.", metricsStoreName, e);
      throw new RuntimeException(e);
    }
  }

  private void logMetrics(String message, MetricsReport metricsReport) {
    LOG.info("{} {}.", message, icebergMetricsFormatter.toPrintableString(metricsReport));
  }

  private void doRecordMetric(MetricsReport metricsReport) {
    try {
      icebergMetricsStore.recordMetric(metricsReport);
    } catch (Exception e) {
      LOG.warn("Write Iceberg metrics failed.", e);
    }
  }
}
