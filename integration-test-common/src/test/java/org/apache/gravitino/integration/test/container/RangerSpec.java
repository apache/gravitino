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

import java.util.List;
import org.apache.gravitino.integration.test.util.TestDatabaseName;

/** The Ranger admin server. There is one configuration, so the spec has no fields. */
public record RangerSpec() implements ContainerSpec<RangerContainer> {

  /** The MySQL database Ranger keeps its policies in. */
  static final MySQLSpec DATABASE =
      new MySQLSpec(MySQLVersion.MYSQL_8_0, TestDatabaseName.MYSQL_RANGER);

  @Override
  public List<ContainerSpec<?>> dependencies() {
    return List.of(DATABASE);
  }

  @Override
  public Class<RangerContainer> containerType() {
    return RangerContainer.class;
  }

  @Override
  public RangerContainer getOrStart(SharedContainers shared) {
    shared.startRangerContainer();
    return shared.getRangerContainer();
  }

  @Override
  public boolean supportsPrivate() {
    return true;
  }

  // Ranger's setup drops and recreates its database, so a private Ranger cannot share the shared
  // one's database on the run's MySQL; it keeps its policies in the image's own MariaDB instead.
  @Override
  public RangerContainer startPrivate(SharedContainers shared, Durability durability) {
    return shared.startPrivate(RangerContainer.builder(), durability);
  }

  static final class Factory implements ContainerFixture.SpecFactory<RangerFixture> {
    @Override
    public Scope scopeFor(RangerFixture fixture) {
      return fixture.scope();
    }

    @Override
    public Durability durabilityFor(RangerFixture fixture) {
      return fixture.durability();
    }

    @Override
    public Startup startupFor(RangerFixture fixture) {
      return fixture.startup();
    }

    @Override
    public RangerSpec specFor(RangerFixture fixture) {
      return new RangerSpec();
    }
  }
}
