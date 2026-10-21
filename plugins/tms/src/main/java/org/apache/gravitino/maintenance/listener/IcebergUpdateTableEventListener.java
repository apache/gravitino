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
package org.apache.gravitino.maintenance.listener;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.maintenance.scheduler.commit.CommitJobScheduler;
import org.apache.gravitino.maintenance.scheduler.commit.RelationalCommitJobScheduler;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upserts a one-shot {@code commit-job} / {@code {table_id}} after a successful IRC table update
 * (§5.2.3).
 *
 * <p>Register with {@code gravitino.eventListener.names} / {@code .{name}.class}. Enqueue builds
 * the onCommit chain snapshot (cooldown gate + jobConf) into {@code task_data}; the commit pool
 * consumes that snapshot and submits Spark.
 *
 * <p>Uses the event class name (not a compile dependency on {@code iceberg-rest-server}) so TMS can
 * load when IRC events are absent from the classpath.
 */
public class IcebergUpdateTableEventListener implements EventListenerPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergUpdateTableEventListener.class);

  /** FQCN of the IRC post-event that wakes commit maintenance. */
  static final String ICEBERG_UPDATE_TABLE_EVENT =
      "org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent";

  private CommitJobScheduler commitJobScheduler;
  private Function<NameIdentifier, TableEntity> tableLookupOverride;

  @Override
  public void init(Map<String, String> properties) throws RuntimeException {
    // No listener-specific properties yet.
  }

  @Override
  public void start() throws RuntimeException {
    DataSource dataSource = resolveEntityStoreDataSource();
    Config gravitinoConfig = GravitinoEnv.getInstance().config();
    if (gravitinoConfig == null) {
      throw new IllegalStateException("GravitinoEnv config is not available");
    }
    this.commitJobScheduler = new RelationalCommitJobScheduler(dataSource, gravitinoConfig);
    LOG.info("TMS IcebergUpdateTableEventListener started (commit-job upsert with snapshot)");
  }

  @Override
  public void stop() throws RuntimeException {
    commitJobScheduler = null;
  }

  @Override
  public Mode mode() {
    return Mode.ASYNC_ISOLATED;
  }

  @Override
  public void onPostEvent(Event postEvent) throws RuntimeException {
    if (postEvent == null || !ICEBERG_UPDATE_TABLE_EVENT.equals(postEvent.getClass().getName())) {
      return;
    }
    enqueueCommitJob(postEvent.identifier());
  }

  /**
   * Resolves {@code identifier} to a table entity id and upserts {@code commit-job}.
   *
   * @param identifier Gravitino table name identifier from the IRC event
   */
  @VisibleForTesting
  void enqueueCommitJob(NameIdentifier identifier) {
    CommitJobScheduler scheduler = commitJobScheduler;
    if (scheduler == null) {
      LOG.debug("Ignoring IcebergUpdateTableEvent; commit-job scheduler is not started");
      return;
    }
    if (identifier == null) {
      return;
    }
    try {
      long tableId = resolveTableId(identifier);
      boolean inserted = scheduler.upsertDueNow(tableId);
      if (inserted) {
        LOG.info("Upserted commit-job for table {} ({})", tableId, identifier);
      } else {
        LOG.debug("commit-job already queued for table {} ({})", tableId, identifier);
      }
    } catch (NoSuchEntityException e) {
      LOG.debug("No Gravitino table for IRC update {}: {}", identifier, e.getMessage());
    } catch (RuntimeException e) {
      LOG.warn("Failed to upsert commit-job for IRC update {}: {}", identifier, e.toString());
    }
  }

  @VisibleForTesting
  void setCommitJobScheduler(CommitJobScheduler commitJobScheduler) {
    this.commitJobScheduler = commitJobScheduler;
  }

  @VisibleForTesting
  void setTableLookup(Function<NameIdentifier, TableEntity> tableLookup) {
    this.tableLookupOverride = tableLookup;
  }

  private long resolveTableId(NameIdentifier identifier) {
    if (tableLookupOverride != null) {
      TableEntity table = tableLookupOverride.apply(identifier);
      if (table == null) {
        throw new NoSuchEntityException("table not found: %s", identifier);
      }
      return table.id();
    }
    TableEntity table = TableMetaService.getInstance().getTableByIdentifier(identifier);
    return table.id();
  }

  private static DataSource resolveEntityStoreDataSource() {
    Object dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    if (!(dataSource instanceof DataSource)) {
      throw new IllegalStateException("Entity store data source is not available");
    }
    return (DataSource) dataSource;
  }
}
