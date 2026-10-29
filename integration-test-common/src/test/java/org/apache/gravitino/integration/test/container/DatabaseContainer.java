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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

/**
 * A database test container backed by a Testcontainers JDBC module, which owns the credentials and
 * the readiness check. This class adds Gravitino's network, logging and {@link Durability} settings
 * on top.
 */
public abstract class DatabaseContainer extends BaseContainer {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseContainer.class);

  /** The Testcontainers module; the same object as {@link #container}. Null when external. */
  @Nullable protected final JdbcDatabaseContainer<?> db;

  /** Wraps {@code db}, which must not be started yet. */
  protected DatabaseContainer(
      JdbcDatabaseContainer<?> db,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(db, hostName, ports, extraHosts, filesToMount, envVars, network);
    this.db = db;
  }

  /**
   * Wraps a database that runs outside this JVM's Testcontainers.
   *
   * @param endpoints its endpoints
   * @param hostName the host name it would have had as a managed container
   */
  protected DatabaseContainer(TestEndpoints.Database endpoints, String hostName) {
    super(endpoints, hostName);
    this.db = null;
  }

  /**
   * Keeps the data directory on {@code tmpfs}, and starts the server with {@code command} if one is
   * given; for {@link #applyDisposableData} overrides. Callers also pin the server's data directory
   * to {@code dataDir}, so a newer image cannot place the data outside the mount.
   */
  protected void keepDataInMemory(String dataDir, String... command) {
    db.withTmpFs(Map.of(dataDir, DISPOSABLE_DATA_TMPFS));
    if (command.length > 0) {
      db.withCommand(command);
    }
  }

  // A Testcontainers JDBC module has its own readiness check: PostgreSQL's waits for a log line,
  // which a port wait would replace; MySQL's queries the database.
  @Override
  @Nullable
  protected WaitStrategy waitStrategy() {
    return null;
  }

  /** The user tests connect as. */
  public String getUsername() {
    return isExternal() ? endpoints(TestEndpoints.Database.class).user() : db.getUsername();
  }

  /** The password of {@link #getUsername()}. */
  public String getPassword() {
    return isExternal() ? endpoints(TestEndpoints.Database.class).password() : db.getPassword();
  }

  /** The Testcontainers module waits for the database itself, so there is nothing left to check. */
  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    return isExternal() || db.isRunning();
  }

  /**
   * Logs whether lower durability is enabled and the settings the server reports, so each run's
   * output shows which mode it ran in. Warns when lower durability is enabled but the data
   * directory is not on {@code tmpfs}. The query must return one row with the data directory first,
   * in the order of {@code names}.
   */
  protected void logDurability(List<String> names, String query) {
    if (isExternal()) {
      return;
    }
    String name = getClass().getSimpleName();
    String[] values = new String[names.size()];
    try (Connection connection = db.createConnection("");
        Statement statement = connection.createStatement();
        ResultSet row = statement.executeQuery(query)) {
      if (!row.next() || row.getMetaData().getColumnCount() != names.size()) {
        LOG.warn("{}: could not read the durability settings from: {}", name, query);
        return;
      }
      for (int i = 0; i < values.length; i++) {
        values[i] = row.getString(i + 1);
      }
    } catch (SQLException e) {
      LOG.warn("{}: could not read the durability settings: {}", name, e.getMessage());
      return;
    }
    String report =
        IntStream.range(0, names.size())
            .mapToObj(i -> names.get(i) + "=" + values[i])
            .collect(Collectors.joining(", "));
    if (durability() == Durability.DURABLE) {
      LOG.info("{}: lower durability disabled, so tests run slower: {}", name, report);
      return;
    }
    Map<String, String> tmpfs = db.getContainerInfo().getHostConfig().getTmpFs();
    if (isOnTmpfs(values[0], tmpfs == null ? Set.of() : tmpfs.keySet())) {
      LOG.info("{}: lower durability enabled, data on tmpfs: {}", name, report);
    } else {
      LOG.warn(
          "{}: lower durability enabled, but data is not on tmpfs, so tests run slower: {}",
          name,
          report);
    }
  }

  /**
   * Returns whether {@code dataDir} is one of the container's {@code tmpfs} mounts or below one.
   * The mounts come from the container's configuration, so the check runs nothing in the container.
   *
   * @param dataDir the data directory the server reports, possibly with a trailing slash
   * @param tmpfsMounts the container's {@code tmpfs} mount points
   * @return true when the data directory is on {@code tmpfs}
   */
  static boolean isOnTmpfs(@Nullable String dataDir, Collection<String> tmpfsMounts) {
    if (dataDir == null) {
      return false;
    }
    String dir = stripTrailingSlashes(dataDir);
    return tmpfsMounts.stream()
        .map(DatabaseContainer::stripTrailingSlashes)
        .anyMatch(mount -> dir.equals(mount) || dir.startsWith(mount + "/"));
  }

  private static String stripTrailingSlashes(String path) {
    return path.replaceAll("/+$", "");
  }

  /** Builder for a {@link DatabaseContainer}. */
  protected abstract static class Builder<
          SELF extends Builder<SELF, CONTAINER>, CONTAINER extends DatabaseContainer>
      extends BaseContainer.Builder<SELF, CONTAINER> {
    protected Durability durability = Durability.runDefault();

    /** Sets the durability; {@link Durability#runDefault()} when not set. */
    public SELF withDurability(Durability durability) {
      this.durability = durability;
      return self;
    }
  }
}
