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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;

/**
 * A database test container backed by a Testcontainers JDBC module, which owns the credentials and
 * the readiness check. This class adds Gravitino's network, logging and {@link Durability} settings
 * on top.
 */
public abstract class DatabaseContainer extends BaseContainer {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseContainer.class);

  /**
   * System property a test task sets to {@code true} when its database data may be lost; {@link
   * Durability#fromSystemProperty()} then returns {@link Durability#DISPOSABLE}.
   */
  public static final String DISPOSABLE_DATA_PROPERTY = "gravitino.test.db.disposableData";

  /**
   * {@code tmpfs} mount for a disposable data directory. 2 GB is a ceiling; the mount only takes
   * what the database writes.
   */
  private static final String DISPOSABLE_DATA_TMPFS = "rw,size=2g";

  /** How much of its durability work a database container keeps. */
  public enum Durability {
    /** Durable settings: the server's own defaults and our config for it. */
    DEFAULT,
    /** Data in memory ({@code tmpfs}) and commits not synced to disk. */
    DISPOSABLE;

    /** Returns {@link #DISPOSABLE} when {@link #DISPOSABLE_DATA_PROPERTY} is set in this JVM. */
    public static Durability fromSystemProperty() {
      return Boolean.getBoolean(DISPOSABLE_DATA_PROPERTY) ? DISPOSABLE : DEFAULT;
    }
  }

  /** The Testcontainers module; the same object as {@link #container}. */
  protected final JdbcDatabaseContainer<?> db;

  private final Durability durability;

  /** Wraps {@code db}, which must not be started yet. */
  protected DatabaseContainer(
      JdbcDatabaseContainer<?> db,
      Durability durability,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(db, hostName, ports, extraHosts, filesToMount, envVars, network);
    this.db = db;
    this.durability = durability;
  }

  /**
   * Keeps the data directory on {@code tmpfs}, and starts the server with {@code command} if one is
   * given, when this container is {@link Durability#DISPOSABLE}. Callers also pin the server's data
   * directory to {@code dataDir}, so a newer image cannot place the data outside the mount.
   */
  protected void applyDurability(String dataDir, String... command) {
    if (durability == Durability.DISPOSABLE) {
      db.withTmpFs(Map.of(dataDir, DISPOSABLE_DATA_TMPFS));
      if (command.length > 0) {
        db.withCommand(command);
      }
    }
  }

  /** The user tests connect as. */
  public String getUsername() {
    return db.getUsername();
  }

  /** The password of {@link #getUsername()}. */
  public String getPassword() {
    return db.getPassword();
  }

  /** The Testcontainers module waits for the database itself, so there is nothing left to check. */
  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    return db.isRunning();
  }

  /**
   * Logs whether lower durability is enabled and the settings the server reports, so each run's
   * output shows which mode it ran in. Warns when lower durability is enabled but the data
   * directory is not on {@code tmpfs}. The query must print tab-separated values, the data
   * directory first, in the order of {@code names}.
   */
  protected void logDurability(List<String> names, String... query) {
    String name = getClass().getSimpleName();
    Container.ExecResult settings = executeInContainer(query);
    String[] values = settings.getStdout().trim().split("\t");
    if (settings.getExitCode() != 0 || values.length != names.size()) {
      LOG.warn("{}: could not read the durability settings: {}", name, settings.getStderr());
      return;
    }
    String report =
        IntStream.range(0, names.size())
            .mapToObj(i -> names.get(i) + "=" + values[i])
            .collect(Collectors.joining(", "));
    if (durability == Durability.DEFAULT) {
      LOG.info("{}: lower durability disabled, so tests run slower: {}", name, report);
      return;
    }
    Container.ExecResult stat = executeInContainer("stat", "-f", "-c", "%T", values[0]);
    String fileSystem =
        stat.getExitCode() == 0 ? stat.getStdout().trim() : "an unknown file system";
    if ("tmpfs".equals(fileSystem)) {
      LOG.info("{}: lower durability enabled, data on tmpfs: {}", name, report);
    } else {
      LOG.warn(
          "{}: lower durability enabled, but data is on {}, not tmpfs, so tests run slower: {}",
          name,
          fileSystem,
          report);
    }
  }

  /** Builder for a {@link DatabaseContainer}. */
  protected abstract static class Builder<
          SELF extends Builder<SELF, CONTAINER>, CONTAINER extends DatabaseContainer>
      extends BaseContainer.Builder<SELF, CONTAINER> {
    protected Durability durability = Durability.DEFAULT;

    /** Sets the durability; {@link Durability#DEFAULT} when not set. */
    public SELF withDurability(Durability durability) {
      this.durability = durability;
      return self;
    }
  }
}
