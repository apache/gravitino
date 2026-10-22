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

/** The ZooKeeper server. There is one configuration, so the spec has no fields. */
public record ZooKeeperSpec() implements ContainerSpec<ZooKeeperContainer> {

  @Override
  public Class<ZooKeeperContainer> containerType() {
    return ZooKeeperContainer.class;
  }

  @Override
  public ZooKeeperContainer getOrStart(SharedContainers shared) {
    shared.startZooKeeperContainer();
    return shared.getZooKeeperContainer();
  }

  @Override
  public boolean supportsPrivate() {
    return true;
  }

  @Override
  public ZooKeeperContainer startPrivate(SharedContainers shared, Durability durability) {
    return shared.startPrivate(SharedContainers.zooKeeperBuilder(), durability);
  }

  static final class Factory implements ContainerFixture.SpecFactory<ZooKeeperFixture> {
    @Override
    public Scope scopeFor(ZooKeeperFixture fixture) {
      return fixture.scope();
    }

    @Override
    public Durability durabilityFor(ZooKeeperFixture fixture) {
      return fixture.durability();
    }

    @Override
    public Startup startupFor(ZooKeeperFixture fixture) {
      return fixture.startup();
    }

    @Override
    public ZooKeeperSpec specFor(ZooKeeperFixture fixture) {
      return new ZooKeeperSpec();
    }
  }
}
