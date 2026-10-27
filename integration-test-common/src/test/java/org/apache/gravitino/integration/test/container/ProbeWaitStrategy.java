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
package org.apache.gravitino.integration.test.container;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

/**
 * Waits until a client-side probe, such as a JDBC query, an API call or a command in the container,
 * says the service is ready. The probe runs at once and then every poll interval until it passes or
 * the startup timeout ends. A probe that throws counts as not ready; the last exception becomes the
 * cause of the timeout.
 */
final class ProbeWaitStrategy extends AbstractWaitStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(ProbeWaitStrategy.class);

  /** A readiness check against the started container. */
  @FunctionalInterface
  interface Probe {
    /**
     * Returns whether the service is ready.
     *
     * @param target the container being started
     * @return true when ready
     * @throws Exception when the service cannot be reached yet; counts as not ready
     */
    boolean isReady(WaitStrategyTarget target) throws Exception;
  }

  /** Decides from the result of a readiness query whether the service is ready. */
  @FunctionalInterface
  interface ResultCheck {
    /**
     * Returns whether the result says the service is ready.
     *
     * @param resultSet the result, before its first row
     * @return true when ready
     * @throws SQLException when the result cannot be read
     */
    boolean isReady(ResultSet resultSet) throws SQLException;
  }

  private final String description;
  private final Probe probe;
  private Duration pollInterval = Duration.ofSeconds(1);
  private Consumer<WaitStrategyTarget> onTimeout = target -> {};

  /**
   * Creates the strategy.
   *
   * @param description what is awaited, such as "Ranger to answer its plugins API"; it names the
   *     condition in the log and in the timeout message
   * @param probe the readiness check
   */
  ProbeWaitStrategy(String description, Probe probe) {
    this.description = requireNonNull(description, "description is null");
    this.probe = requireNonNull(probe, "probe is null");
  }

  /**
   * Sets how long to wait between probes; one second unless set.
   *
   * @param pollInterval the interval
   * @return this strategy
   */
  ProbeWaitStrategy withPollInterval(Duration pollInterval) {
    this.pollInterval = requireNonNull(pollInterval, "pollInterval is null");
    return this;
  }

  /**
   * Sets what to run when the probe has not passed by the startup timeout, before the start fails,
   * such as logging the service's own status report.
   *
   * @param onTimeout the action, given the container being started
   * @return this strategy
   */
  ProbeWaitStrategy withTimeoutReport(Consumer<WaitStrategyTarget> onTimeout) {
    this.onTimeout = requireNonNull(onTimeout, "onTimeout is null");
    return this;
  }

  /**
   * Returns a probe that connects over JDBC, runs a query and checks its result. A failed
   * connection or query counts as not ready.
   *
   * @param jdbcUrl the JDBC URL of the container being started
   * @param user the user to connect as
   * @param password the password
   * @param query the readiness query
   * @param check decides from the result whether the service is ready
   * @return the probe
   */
  static Probe jdbcQuery(
      Function<WaitStrategyTarget, String> jdbcUrl,
      String user,
      String password,
      String query,
      ResultCheck check) {
    return target -> {
      try (Connection connection =
              DriverManager.getConnection(jdbcUrl.apply(target), user, password);
          Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(query)) {
        return check.isReady(resultSet);
      }
    };
  }

  @Override
  protected void waitUntilReady() {
    WaitStrategyTarget target = waitStrategyTarget;
    AtomicReference<Exception> lastFailure = new AtomicReference<>();
    long startNanos = System.nanoTime();
    try {
      Awaitility.await(description)
          .atMost(startupTimeout)
          .pollDelay(Duration.ZERO)
          .pollInterval(pollInterval)
          // Fail on this probe's timeout only, not on an exception thrown in some other thread.
          .dontCatchUncaughtExceptions()
          .until(() -> probeOnce(target, lastFailure));
    } catch (ConditionTimeoutException e) {
      onTimeout.accept(target);
      Exception cause = lastFailure.get();
      throw new ContainerLaunchException(
          String.format("Timed out after %s waiting for %s", startupTimeout, description),
          cause != null ? cause : e);
    }
    LOG.info("Ready after {} ms: {}", (System.nanoTime() - startNanos) / 1_000_000, description);
  }

  private boolean probeOnce(WaitStrategyTarget target, AtomicReference<Exception> lastFailure) {
    try {
      if (probe.isReady(target)) {
        return true;
      }
      LOG.debug("Not ready yet: {}", description);
    } catch (Exception e) {
      lastFailure.set(e);
      LOG.debug("Not ready yet: {}: {}", description, e.toString());
    }
    return false;
  }
}
