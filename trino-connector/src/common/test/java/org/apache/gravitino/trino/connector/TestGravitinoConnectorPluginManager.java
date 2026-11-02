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
package org.apache.gravitino.trino.connector;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestGravitinoConnectorPluginManager {

  @Test
  void testStarburstAiModelUsesApplicationClassLoader() {
    assertThat(GravitinoConnectorPluginManager.PARENT_FIRST_PACKAGES_FALLBACK)
        .contains("io.starburst.ai.model.");
  }

  @Test
  void testLoadParentFirstPackagesReadsTrinoOwnList() {
    // io.trino.server.PluginManager.SPI_PACKAGES is on the test classpath via trino-main; reading
    // it should return Trino's own list for this module's Trino version, not just fall back to
    // our hardcoded snapshot (which was never updated for org.locationtech.jts., added in Trino
    // 482 for the Iceberg connector's Parquet writer).
    assertThat(
            GravitinoConnectorPluginManager.loadParentFirstPackages(
                Thread.currentThread().getContextClassLoader()))
        .contains("io.trino.spi.");
  }

  @Test
  void testLoadParentFirstPackagesFallsBackWhenPluginManagerIsUnavailable() {
    ClassLoader classLoaderWithoutTrinoMain =
        new ClassLoader(null) {
          @Override
          public Class<?> loadClass(String name) throws ClassNotFoundException {
            throw new ClassNotFoundException(name);
          }
        };
    assertThat(GravitinoConnectorPluginManager.loadParentFirstPackages(classLoaderWithoutTrinoMain))
        .isEqualTo(GravitinoConnectorPluginManager.PARENT_FIRST_PACKAGES_FALLBACK);
  }
}
