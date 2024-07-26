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
package org.apache.gravitino.iceberg.integration.test.util;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.gravitino.iceberg.server.GravitinoIcebergRESTServer;

public class IcebergRESTServerManagerForEmbedded extends IcebergRESTServerManager {

  private File mockConfDir;
  private final ExecutorService executor;

  public IcebergRESTServerManagerForEmbedded() {
    try {
      this.mockConfDir = Files.createTempDirectory("MiniIcebergRESTServer").toFile();
      LOG.info("config dir:{}", mockConfDir.getAbsolutePath());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mockConfDir.mkdirs();
    mockConfDir.deleteOnExit();
    this.executor = Executors.newSingleThreadExecutor();
  }

  @Override
  public Path getConfigDir() {
    return mockConfDir.toPath();
  }

  @Override
  public Optional<Future<?>> doStartIcebergRESTServer() {
    Future<?> future =
        executor.submit(
            () -> {
              try {
                GravitinoIcebergRESTServer.main(
                    new String[] {
                      Paths.get(mockConfDir.getAbsolutePath(), GravitinoIcebergRESTServer.CONF_FILE)
                          .toString()
                    });
              } catch (Exception e) {
                LOG.error("Exception in startup mini GravitinoIcebergRESTServer ", e);
                throw new RuntimeException(e);
              }
            });

    return Optional.of(future);
  }

  @Override
  public void doStopIcebergRESTServer() {
    executor.shutdownNow();
  }
}
