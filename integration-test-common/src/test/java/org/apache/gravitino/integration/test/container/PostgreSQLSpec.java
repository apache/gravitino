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

import java.util.Objects;
import org.apache.gravitino.integration.test.util.TestDatabaseName;

/**
 * A PostgreSQL server of one version, with {@code database} created on it. Versions run side by
 * side; every class that asks for a version shares its server.
 */
public record PostgreSQLSpec(PGImageName version, TestDatabaseName database)
    implements ContainerSpec<PostgreSQLContainer> {

  /** Checks that both parts are set. */
  public PostgreSQLSpec {
    Objects.requireNonNull(version, "version");
    Objects.requireNonNull(database, "database");
  }

  @Override
  public Class<PostgreSQLContainer> containerType() {
    return PostgreSQLContainer.class;
  }

  @Override
  public PostgreSQLContainer getOrStart(ContainerSuite suite) {
    suite.startPostgreSQLContainer(database, version);
    return suite.getPostgreSQLContainer(version);
  }

  static final class Factory implements ContainerFixture.SpecFactory<PostgreSQLFixture> {
    @Override
    public Durability durabilityFor(PostgreSQLFixture fixture) {
      return fixture.durability();
    }

    @Override
    public Startup startupFor(PostgreSQLFixture fixture) {
      return fixture.startup();
    }

    @Override
    public PostgreSQLSpec specFor(PostgreSQLFixture fixture) {
      return new PostgreSQLSpec(fixture.version(), fixture.database());
    }
  }
}
