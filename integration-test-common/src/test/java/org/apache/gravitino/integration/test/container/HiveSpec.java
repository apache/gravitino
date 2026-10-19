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
 * different version stops the running Hive of that mode and starts the requested one (see {@link
 * ContainerSlot}). Tests of one version therefore share a container, and a version switch costs a
 * restart, as with direct {@link ContainerSuite} calls.
 */
public record HiveSpec(HiveVersion version, HiveAuth auth) implements ContainerSpec<HiveContainer> {

  @Override
  public Class<HiveContainer> containerType() {
    return HiveContainer.class;
  }

  @Override
  public HiveContainer getOrStart(ContainerSuite suite) {
    Map<String, String> env = Map.of(HiveContainer.HIVE_RUNTIME_VERSION, version.runtimeVersion());
    return switch (auth) {
      case NONE -> {
        suite.startHiveContainer(env);
        yield suite.getHiveContainer();
      }
      case KERBEROS -> {
        suite.startKerberosHiveContainer(env);
        yield suite.getKerberosHiveContainer();
      }
    };
  }

  static final class Factory implements ContainerFixture.SpecFactory<HiveFixture> {
    @Override
    public HiveSpec specFor(HiveFixture fixture) {
      return new HiveSpec(fixture.version(), fixture.auth());
    }
  }
}
