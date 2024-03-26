/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;

import com.datastrato.gravitino.Config;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.dbcp2.DelegatingConnection;
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
      LOG.info("Start to collect and delete old version data by thread {}", threadId);
      collectAndRemoveOldVersionData();
    } catch (Exception e) {
      LOG.error("Thread {} failed to collect and clean garbage.", threadId, e);
    } finally {
      LOG.info("Thread {} finish to collect garbage.", threadId);
    }
  }

  private void collectAndRemoveOldVersionData() throws SQLException {
    long deleteTimeLine = System.currentTimeMillis() - config.get(KV_DELETE_AFTER_TIME);

    for (AllTables.TABLE_NAMES table : AllTables.TABLE_NAMES.values()) {
      System.out.println(table.getTableName());

      /*
      String sql = "SELECT * FROM " + table + " WHERE deleted_at != 0 AND deleted_at < ?";
      DelegatingConnection<Connection> connection = null;
      try (PreparedStatement stmt = connection.prepareStatement(sql)) {
        stmt.setLong(1, deleteTimeLine);
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            String deleteSql = "DELETE FROM " + table + " WHERE id = ?";
            try (PreparedStatement deleteStmt = connection.prepareStatement(deleteSql)) {
              deleteStmt.setLong(1, rs.getLong("id"));
              deleteStmt.executeUpdate();
            }

            String checkSql = "SELECT * FROM " + table + " WHERE id > ? LIMIT 1";
            try (PreparedStatement checkStmt = connection.prepareStatement(checkSql)) {
              checkStmt.setLong(1, rs.getLong("id"));
              try (ResultSet checkRs = checkStmt.executeQuery()) {
                if (checkRs.next()) {
                  String deleteOldVersionSql = "DELETE FROM " + table + " WHERE id = ?";
                  try (PreparedStatement deleteOldVersionStmt =
                      connection.prepareStatement(deleteOldVersionSql)) {
                    deleteOldVersionStmt.setLong(1, rs.getLong("id"));
                    deleteOldVersionStmt.executeUpdate();
                  }
                }
              }
            }
          }
        }
      } */
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
