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

/**
 * A {@link TrinoCluster} of one Trino version and worker count, talking to one Gravitino server.
 *
 * <p>Trino needs the Gravitino server's port when it starts, so the spec from {@link TrinoFixture}
 * has none until {@link #forTestInstance} reads it from a {@link GravitinoServerAware} test; that
 * is why {@code @TrinoFixture} starts lazily. Each server gets its own cluster: a new port replaces
 * the running cluster.
 */
public record TrinoSpec(int version, int workers, HiveVersion hive, int gravitinoServerPort)
    implements ContainerSpec<TrinoCluster> {

  private static final int NO_PORT = -1;

  @Override
  public Class<TrinoCluster> containerType() {
    return TrinoCluster.class;
  }

  @Override
  public ContainerSpec<TrinoCluster> forTestInstance(Object testInstance) {
    if (testInstance instanceof GravitinoServerAware server) {
      return new TrinoSpec(version, workers, hive, server.getGravitinoServerPort());
    }
    return this;
  }

  @Override
  public TrinoCluster getOrStart(SharedContainers shared) {
    if (gravitinoServerPort == NO_PORT) {
      throw new IllegalStateException(
          "Trino needs a Gravitino server: ask for TrinoCluster in a test or @BeforeAll method of a"
              + " class that implements GravitinoServerAware, after the server starts");
    }
    return shared.startTrinoCluster(this);
  }

  static final class Factory implements ContainerFixture.SpecFactory<TrinoFixture> {
    @Override
    public Durability durabilityFor(TrinoFixture fixture) {
      return fixture.durability();
    }

    @Override
    public TrinoSpec specFor(TrinoFixture fixture) {
      return new TrinoSpec(fixture.version(), fixture.workers(), fixture.hive(), NO_PORT);
    }

    @Override
    public Startup startupFor(TrinoFixture fixture) {
      return fixture.startup();
    }
  }
}
