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
package org.apache.gravitino.maintenance.web.rest.feature;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.scheduler.PolicyExpandScheduler;
import org.apache.gravitino.maintenance.scheduler.PolicyTimeTriggerEventListener;
import org.apache.gravitino.maintenance.scheduler.PolicyTimeTriggerEventListener.PolicySnapshot;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jersey feature that starts table maintenance db-scheduler pools when the extension package is
 * enabled (§7.1).
 *
 * <p>TMS does not register public REST resources in this change; the feature only owns server
 * lifecycle for in-process scheduling. While the pools are running, create, alter, enable, disable,
 * and delete of a maintenance policy that carries {@code timeTrigger} insert, move, or delete the
 * matching {@code policy-expand} row. Pools stop on Jersey {@code DESTROY_FINISHED}, which runs
 * during Jetty shutdown before {@code GravitinoEnv} closes the entity-store {@code DataSource}.
 *
 * <p>After {@link #stopSchedulers()}, a later {@link #configure} may start pools again and
 * re-register the destroy listener (needed for tests / embedded restarts in the same JVM).
 */
@Provider
public class TableMaintenanceRESTFeature implements Feature {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceRESTFeature.class);

  /** Package name for {@link Configs#REST_API_EXTENSION_PACKAGES}. */
  public static final String EXTENSION_PACKAGE = TableMaintenanceRESTFeature.class.getPackageName();

  private static final AtomicReference<TableMaintenanceSchedulerPools> SCHEDULER_POOLS =
      new AtomicReference<>();

  private static final AtomicReference<PolicyExpandScheduler> EXPAND_SCHEDULER =
      new AtomicReference<>();

  private static final AtomicBoolean TIME_TRIGGER_LISTENER_REGISTERED = new AtomicBoolean();

  @Override
  public boolean configure(FeatureContext context) {
    if (SCHEDULER_POOLS.get() != null) {
      LOG.warn("Table maintenance scheduler pools are already running; skipping reconfigure");
      return true;
    }

    Config config = GravitinoEnv.getInstance().config();
    if (!Configs.RELATIONAL_ENTITY_STORE.equals(config.get(Configs.ENTITY_STORE))) {
      throw new IllegalStateException(
          "Table maintenance requires gravitino.entity.store = relational");
    }

    DataSource dataSource = resolveEntityStoreDataSource();
    TableMaintenanceSchedulerPools pools = new TableMaintenanceSchedulerPools();
    pools.start(dataSource, TableMaintenanceConfig.from(config));
    if (!SCHEDULER_POOLS.compareAndSet(null, pools)) {
      pools.stop();
      LOG.warn("Table maintenance scheduler pools were started concurrently; discarded duplicate");
      return true;
    }

    try {
      EXPAND_SCHEDULER.set(PolicyExpandScheduler.create(dataSource));
      registerTimeTriggerListener();
    } catch (RuntimeException e) {
      EXPAND_SCHEDULER.set(null);
      SCHEDULER_POOLS.set(null);
      pools.stop();
      throw e;
    }

    // Register only after a successful CAS so a later configure-after-stop can attach a listener
    // again on the new FeatureContext (listener is scoped to the Jersey Application).
    context.register(new SchedulerShutdownListener());
    LOG.info("Table maintenance scheduler pools are running");
    return true;
  }

  /** Stops scheduler pools. Invoked from Jersey destroy and tests. */
  public static void stopSchedulers() {
    EXPAND_SCHEDULER.set(null);
    TableMaintenanceSchedulerPools pools = SCHEDULER_POOLS.getAndSet(null);
    if (pools != null) {
      pools.stop();
      LOG.info("Table maintenance scheduler pools stopped");
    }
  }

  private static void registerTimeTriggerListener() {
    if (!TIME_TRIGGER_LISTENER_REGISTERED.compareAndSet(false, true)) {
      return;
    }
    if (GravitinoEnv.getInstance().eventListenerManager() == null) {
      TIME_TRIGGER_LISTENER_REGISTERED.set(false);
      throw new IllegalStateException("Event listener manager is not initialized");
    }
    GravitinoEnv.getInstance()
        .eventListenerManager()
        .addEventListener(
            "tms-policy-time-trigger",
            new PolicyTimeTriggerEventListener(
                EXPAND_SCHEDULER::get, TableMaintenanceRESTFeature::loadPolicy));
  }

  private static Optional<PolicySnapshot> loadPolicy(String metalake, String policyName) {
    if (GravitinoEnv.getInstance().policyDispatcher() == null) {
      return Optional.empty();
    }
    try {
      PolicyEntity policy =
          GravitinoEnv.getInstance().policyDispatcher().getPolicy(metalake, policyName);
      return Optional.of(new PolicySnapshot(policy.id(), policy.enabled(), policy.content()));
    } catch (NoSuchPolicyException e) {
      return Optional.empty();
    }
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

  /**
   * Stops TMS scheduler pools when the Jersey application is destroyed (Jetty stop), before the
   * relational entity store shuts down.
   */
  static final class SchedulerShutdownListener implements ApplicationEventListener {

    @Override
    public void onEvent(ApplicationEvent event) {
      if (event.getType() == ApplicationEvent.Type.DESTROY_FINISHED) {
        stopSchedulers();
      }
    }

    @Override
    public RequestEventListener onRequest(RequestEvent requestEvent) {
      return null;
    }
  }
}
