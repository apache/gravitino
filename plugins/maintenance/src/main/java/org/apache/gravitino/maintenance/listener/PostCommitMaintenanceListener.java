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

import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.maintenance.scheduler.job.commit.CommitJobClient;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wakes commit maintenance after a successful IRC table update (§5.2.3): filters the event and
 * calls {@link CommitJobClient#enqueueNow}.
 *
 * <p>Register with {@code gravitino.eventListener.names} / {@code .{name}.class}. Uses the event
 * class name (not a compile dependency on {@code iceberg-rest-server}) so TMS can load when IRC
 * events are absent from the classpath.
 */
public class PostCommitMaintenanceListener implements EventListenerPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(PostCommitMaintenanceListener.class);

  /** FQCN of the IRC post-event that wakes commit maintenance. */
  static final String ICEBERG_UPDATE_TABLE_EVENT =
      "org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent";

  private CommitJobClient commitJobClient;

  @Override
  public void init(Map<String, String> properties) throws RuntimeException {
    // No listener-specific properties yet.
  }

  @Override
  public void start() throws RuntimeException {
    Object raw =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    if (!(raw instanceof DataSource)) {
      throw new IllegalStateException("Entity store data source is not available");
    }
    Config gravitinoConfig = GravitinoEnv.getInstance().config();
    if (gravitinoConfig == null) {
      throw new IllegalStateException("GravitinoEnv config is not available");
    }
    this.commitJobClient = new CommitJobClient((DataSource) raw, gravitinoConfig);
    LOG.info("PostCommitMaintenanceListener started");
  }

  @Override
  public void stop() throws RuntimeException {
    commitJobClient = null;
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
    CommitJobClient client = commitJobClient;
    if (client == null) {
      LOG.debug("Ignoring IcebergUpdateTableEvent; commit-job client is not started");
      return;
    }
    client.enqueueNow(postEvent.identifier());
  }
}
