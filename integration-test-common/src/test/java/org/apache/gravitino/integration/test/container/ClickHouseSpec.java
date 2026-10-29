/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import org.apache.gravitino.integration.test.util.TestDatabaseName;

/**
 * A ClickHouse server ({@code nodes} 1) or the three-node ClickHouse cluster with ZooKeeper ({@code
 * nodes} 3), with {@code database} created on it.
 *
 * <p>The database is created on the shared container and is not part of the container identity: two
 * specs that differ only by database share one ClickHouse. The cluster size is fixed by {@link
 * ContainerSuite}, so a variable size needs the cluster start to move out of {@code
 * ContainerSuite}.
 */
public record ClickHouseSpec(int nodes, TestDatabaseName database)
    implements ContainerSpec<ClickHouseContainer> {

  private static final int SINGLE_NODE = 1;
  // Must match ContainerSuite.CLICKHOUSE_CLUSTER_SIZE.
  private static final int CLUSTER_NODES = 3;

  private static final String REMOTE_SERVERS_TEMPLATE = "/remote_servers.xml";

  private static final Supplier<Path> REMOTE_SERVERS =
      Suppliers.memoize(ClickHouseSpec::copyRemoteServersTemplate);

  /**
   * Checks the node count.
   *
   * @throws IllegalArgumentException if {@code nodes} is neither 1 nor 3
   */
  public ClickHouseSpec {
    if (nodes != SINGLE_NODE && nodes != CLUSTER_NODES) {
      throw new IllegalArgumentException(
          String.format(
              "ClickHouse supports %d or %d nodes, not %d", SINGLE_NODE, CLUSTER_NODES, nodes));
    }
    Objects.requireNonNull(database, "database");
  }

  @Override
  public Class<ClickHouseContainer> containerType() {
    return ClickHouseContainer.class;
  }

  @Override
  public ClickHouseContainer getOrStart(ContainerSuite suite) {
    if (nodes == SINGLE_NODE) {
      suite.startClickHouseContainer(database);
      return suite.getClickHouseContainer();
    }
    suite.startClickHouseClusterContainer(database, REMOTE_SERVERS.get().toString());
    return suite.getClickHouseClusterContainer();
  }

  // ContainerSuite reads the template from a file path, and the resource may sit inside a jar.
  // Copied on first use, not in a static initializer, so loading this class never touches the
  // disk; a failed copy is not cached and the next start tries again.
  private static Path copyRemoteServersTemplate() {
    try (InputStream template =
        Objects.requireNonNull(
            ClickHouseSpec.class.getResourceAsStream(REMOTE_SERVERS_TEMPLATE),
            REMOTE_SERVERS_TEMPLATE)) {
      Path path = Files.createTempFile("gravitino-clickhouse-remote-servers-", ".xml");
      path.toFile().deleteOnExit();
      Files.copy(template, path, StandardCopyOption.REPLACE_EXISTING);
      return path;
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot copy " + REMOTE_SERVERS_TEMPLATE, e);
    }
  }

  static final class Factory implements ContainerFixture.SpecFactory<ClickHouseFixture> {
    @Override
    public Startup startupFor(ClickHouseFixture fixture) {
      return fixture.startup();
    }

    @Override
    public ClickHouseSpec specFor(ClickHouseFixture fixture) {
      return new ClickHouseSpec(fixture.nodes(), fixture.database());
    }
  }
}
