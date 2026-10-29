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

import static java.lang.String.format;

import com.google.common.collect.ImmutableSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

public class StarRocksContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(StarRocksContainer.class);

  public static final String DEFAULT_IMAGE = TestImages.STARROCKS;
  public static final String HOST_NAME = "gravitino-ci-starrocks";
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "";
  public static final int FE_HTTP_PORT = 8030;
  public static final int FE_MYSQL_PORT = 9030;

  // The allin1 image installs StarRocks under /data/deploy/starrocks, creates these two directories
  // empty at build time and keeps the default meta_dir and storage_root_path.
  private static final String FE_META_DIR = "/data/deploy/starrocks/fe/meta";
  private static final String BE_STORAGE_DIR = "/data/deploy/starrocks/be/storage";
  // The FE refuses to start when its metadata directory has less than 5 GB free
  // (MetaHelper.checkMetaDir); tmpfs only uses memory for what is written.
  private static final String FE_META_TMPFS = "rw,size=6g";

  public static Builder builder() {
    return new Builder();
  }

  protected StarRocksContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  // With the metadata and tablets on tmpfs, the FE's BDB JE syncs and the BE's fsyncs cost
  // nothing, so the FE sync policy can stay as the image sets it, unlike Doris, whose FE metadata
  // stays on disk.
  @Override
  protected void applyDisposableData(GenericContainer<?> container) {
    container.withTmpFs(Map.of(FE_META_DIR, FE_META_TMPFS, BE_STORAGE_DIR, DISPOSABLE_DATA_TMPFS));
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "StarRocksContainer")));
  }

  // The FE answers before a BE has registered; ready means one live BE with storage.
  @Override
  protected WaitStrategy waitStrategy() {
    return new WaitAllStrategy()
        .withStrategy(Wait.forListeningPort())
        .withStrategy(
            new ProbeWaitStrategy(
                "StarRocks to report a live backend with capacity",
                ProbeWaitStrategy.jdbcQuery(
                    target ->
                        format(
                            "jdbc:mysql://%s:%d/",
                            ipAddress(target.getContainerInfo()), FE_MYSQL_PORT),
                    USER_NAME,
                    PASSWORD,
                    "SHOW PROC '/backends';",
                    resultSet -> readyBackends(resultSet) > 0)));
  }

  // The port check's five minutes plus the 30 seconds the backend check had before.
  @Override
  protected Duration startupTimeout() {
    return Duration.ofMinutes(5).plusSeconds(30);
  }

  /**
   * Counts the backends in a {@code SHOW PROC '/backends'} result that are alive and report
   * storage. Doris answers the same query in the same shape.
   *
   * @param backends the query result
   * @return the number of ready backends
   * @throws SQLException when the result cannot be read
   */
  static int readyBackends(ResultSet backends) throws SQLException {
    int ready = 0;
    while (backends.next()) {
      String alive = backends.getString("Alive");
      String totalCapacity = backends.getString("TotalCapacity");
      // TotalCapacity reads like "1.234 TB"; a BE that has not reported its disks shows 0.000.
      if ("true".equalsIgnoreCase(alive)
          && totalCapacity != null
          && Float.parseFloat(totalCapacity.split(" ")[0]) > 0.0f) {
        ready++;
      }
    }
    return ready;
  }

  public static class Builder
      extends BaseContainer.Builder<StarRocksContainer.Builder, StarRocksContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(FE_HTTP_PORT, FE_MYSQL_PORT);
    }

    @Override
    public StarRocksContainer build() {
      return new StarRocksContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
