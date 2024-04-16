/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT;
import static com.datastrato.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.VERSION_RETENTION_COUNT;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.storage.relational.po.FilesetVersionPO;
import com.datastrato.gravitino.storage.relational.service.CatalogMetaService;
import com.datastrato.gravitino.storage.relational.service.FilesetMetaService;
import com.datastrato.gravitino.storage.relational.service.MetalakeMetaService;
import com.datastrato.gravitino.storage.relational.service.SchemaMetaService;
import com.datastrato.gravitino.storage.relational.service.TableMetaService;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
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
    long dateTimeLineMinute = config.get(STORE_DELETE_AFTER_TIME) / 1000 / 60;

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
    long legacyTimeLine = System.currentTimeMillis() - config.get(STORE_DELETE_AFTER_TIME);

    for (AllTables.TABLE_NAMES tableName : AllTables.TABLE_NAMES.values()) {
      switch (tableName) {
        case METALAKE_TABLE_NAME:
          MetalakeMetaService.getInstance()
              .deleteMetalakeMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
        case CATALOG_TABLE_NAME:
          CatalogMetaService.getInstance()
              .deleteCatalogMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
        case SCHEMA_TABLE_NAME:
          SchemaMetaService.getInstance()
              .deleteSchemaMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
        case TABLE_TABLE_NAME:
          TableMetaService.getInstance()
              .deleteTableMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
        case FILESET_TABLE_NAME:
          FilesetMetaService.getInstance()
              .deleteFilesetAndVersionMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
        case FILESET_VERSION_TABLE_NAME:
          FilesetMetaService.getInstance()
              .deleteFilesetAndVersionMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
        default:
          throw new IllegalArgumentException(
              "Unsupported table name when collectAndRemoveLegacyData: " + tableName);
      }
    }
  }

  private void collectAndRemoveOldVersionData() {
    long version_retention_count = config.get(VERSION_RETENTION_COUNT);

    for (AllTables.TABLE_NAMES tableName : AllTables.TABLE_NAMES.values()) {
      switch (tableName) {
        case FILESET_VERSION_TABLE_NAME:
          List<FilesetVersionPO> filesetCurVersions =
              FilesetMetaService.getInstance()
                  .getFilesetVersionPOsByRetentionCount(version_retention_count);

          for (FilesetVersionPO filesetVersionPO : filesetCurVersions) {
            long versionRetentionLine =
                filesetVersionPO.getVersion().longValue() - version_retention_count;
            FilesetMetaService.getInstance()
                .deleteFilesetVersionsByRetentionLine(
                    filesetVersionPO.getFilesetId(),
                    versionRetentionLine,
                    GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
          }
        case METALAKE_TABLE_NAME:
        case CATALOG_TABLE_NAME:
        case SCHEMA_TABLE_NAME:
        case TABLE_TABLE_NAME:
        case FILESET_TABLE_NAME:
          continue;
        default:
          throw new IllegalArgumentException(
              "Unsupported table name when collectAndRemoveOldVersionData: " + tableName);
      }
    }
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
