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
 * A Doris server of one version, given by {@code version}. Versions run side by side, each under
 * its own host name.
 */
public record DorisSpec(DorisImageName version) implements ContainerSpec<DorisContainer> {

  @Override
  public Class<DorisContainer> containerType() {
    return DorisContainer.class;
  }

  @Override
  public DorisContainer getOrStart(SharedContainers shared) {
    shared.startDorisContainer(version);
    return shared.getDorisContainer(version);
  }

  @Override
  public boolean supportsPrivate() {
    return true;
  }

  @Override
  public DorisContainer startPrivate(SharedContainers shared, Durability durability) {
    return shared.startPrivate(SharedContainers.dorisBuilder(version), durability);
  }

  static final class Factory implements ContainerFixture.SpecFactory<DorisFixture> {
    @Override
    public Scope scopeFor(DorisFixture fixture) {
      return fixture.scope();
    }

    @Override
    public Durability durabilityFor(DorisFixture fixture) {
      return fixture.durability();
    }

    @Override
    public Startup startupFor(DorisFixture fixture) {
      return fixture.startup();
    }

    @Override
    public DorisSpec specFor(DorisFixture fixture) {
      return new DorisSpec(fixture.version());
    }
  }
}
