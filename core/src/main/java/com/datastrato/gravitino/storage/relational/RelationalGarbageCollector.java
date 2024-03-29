/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.storage.relational.service.CatalogMetaService;
import com.datastrato.gravitino.storage.relational.service.FilesetMetaService;
import com.datastrato.gravitino.storage.relational.service.MetalakeMetaService;
import com.datastrato.gravitino.storage.relational.service.SchemaMetaService;
import com.datastrato.gravitino.storage.relational.service.TableMetaService;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RelationalGarbageCollector implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(RelationalGarbageCollector.class);
  private final RelationalBackend backend;

  private final Config config;

  @VisibleForTesting
  final ScheduledExecutorService garbageCollectorPool =
      new ScheduledThreadPoolExecutor(
          2,
          r -> {
            Thread t = new Thread(r, "RelationalBackend-Garbage-Collector");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());

  public RelationalGarbageCollector(RelationalBackend backend, Config config) {
    this.backend = backend;
    this.config = config;
  }

  public void start() {
    long dateTimeLineMinute = config.get(KV_DELETE_AFTER_TIME) / 1000 / 60;

    // We will collect garbage every 10 minutes at least. If the dateTimeLineMinute is larger than
    // 100 minutes, we would collect garbage every dateTimeLineMinute/10 minutes.
    long frequency = Math.max(dateTimeLineMinute / 10, 10);
    garbageCollectorPool.scheduleAtFixedRate(this::collectAndClean, 5, frequency, TimeUnit.MINUTES);
  }

  private void collectAndClean() {
    long threadId = Thread.currentThread().getId();
    LOG.info("Thread {} start to collect garbage...", threadId);
    try {
      LOG.info("Start to collect and delete legacy data by thread {}", threadId);
      collectAndRemoveLegacyData();
      LOG.info("Start to collect and delete old version data by thread {}", threadId);
      collectAndRemoveOldVersionData();
    } catch (Exception e) {
      LOG.error("Thread {} failed to collect and clean garbage.", threadId, e);
    } finally {
      LOG.info("Thread {} finish to collect garbage.", threadId);
    }
  }

  private void collectAndRemoveLegacyData() throws SQLException {
    long legacyTimeLine = System.currentTimeMillis() - config.get(KV_DELETE_AFTER_TIME);
    // TODO: put limit to configuration
    int limit = 20;

    for (AllTables.TABLE_NAMES tableName : AllTables.TABLE_NAMES.values()) {
      switch (tableName) {
        case METALAKE_TABLE_NAME:
          MetalakeMetaService.getInstance()
              .deleteMetalakeMetasByLegacyTimeLine(legacyTimeLine, limit);
        case CATALOG_TABLE_NAME:
          CatalogMetaService.getInstance()
              .deleteCatalogMetasByLegacyTimeLine(legacyTimeLine, limit);
        case SCHEMA_TABLE_NAME:
          SchemaMetaService.getInstance().deleteSchemaMetasByLegacyTimeLine(legacyTimeLine, limit);
        case TABLE_TABLE_NAME:
          TableMetaService.getInstance().deleteTableMetasByLegacyTimeLine(legacyTimeLine, limit);
        case FILESET_TABLE_NAME:
          FilesetMetaService.getInstance()
              .deleteFilesetAndVersionMetasByLegacyTimeLine(legacyTimeLine, limit);
        case FILESET_VERSION_TABLE_NAME:
          FilesetMetaService.getInstance()
              .deleteFilesetAndVersionMetasByLegacyTimeLine(legacyTimeLine, limit);
        default:
          throw new IllegalArgumentException("Unsupported table name: " + tableName);
      }
    }
  }

  private void collectAndRemoveOldVersionData() {
    long deleteTimeLine = System.currentTimeMillis() - config.get(KV_DELETE_AFTER_TIME);

    int version_retention_count = 1;


    for (AllTables.TABLE_NAMES table : AllTables.TABLE_NAMES.values()) {}
  }

  @Override
  public void close() throws IOException {
    this.garbageCollectorPool.shutdown();
    try {
      if (!this.garbageCollectorPool.awaitTermination(5, TimeUnit.SECONDS)) {
        this.garbageCollectorPool.shutdownNow();
      }
    } catch (InterruptedException ex) {
      this.garbageCollectorPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
