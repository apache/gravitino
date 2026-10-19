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

import java.util.Map;

/**
 * A Hive server of one version and authentication mode.
 *
 * <p>Plain and Kerberos Hive run side by side, but each holds one version at a time: asking for a
 * different version stops the running Hive of that mode and starts the requested one (see {@code
 * ContainerSlot}). Tests of one version therefore share a container, and a version switch costs a
 * restart, as with direct {@link ContainerSuite} calls. A private Hive ({@link Scope#CLASS} or
 * {@link Scope#METHOD}) leaves the shared one alone.
 */
public record HiveSpec(HiveVersion version, HiveAuth auth) implements ContainerSpec<HiveContainer> {

  @Override
  public Class<HiveContainer> containerType() {
    return HiveContainer.class;
  }

  @Override
  public HiveContainer getOrStart(ContainerSuite suite) {
    return switch (auth) {
      case NONE -> {
        suite.startHiveContainer(env());
        yield suite.getHiveContainer();
      }
      case KERBEROS -> {
        suite.startKerberosHiveContainer(env());
        yield suite.getKerberosHiveContainer();
      }
    };
  }

  // Keeps the shared Hive's host name: the Kerberos image derives its principals from it. On macOS
  // the shared network then has two containers with one alias, which is fine because tests reach
  // containers by IP; on Linux CI there is no shared network.
  @Override
  public HiveContainer startPrivate(ContainerSuite suite) {
    return suite.startPrivate(network -> builder().withNetwork(network).build());
  }

  private HiveContainer.Builder builder() {
    return switch (auth) {
      case NONE -> ContainerSuite.hiveBuilder(env());
      case KERBEROS -> ContainerSuite.kerberosHiveBuilder(env());
    };
  }

  private Map<String, String> env() {
    return Map.of(HiveContainer.HIVE_RUNTIME_VERSION, version.runtimeVersion());
  }

  static final class Factory implements ContainerFixture.SpecFactory<HiveFixture> {
    @Override
    public HiveSpec specFor(HiveFixture fixture) {
      return new HiveSpec(fixture.version(), fixture.auth());
    }

    @Override
    public Scope scopeFor(HiveFixture fixture) {
      return fixture.scope();
    }
  }
}
