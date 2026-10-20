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
 * A MySQL server of one version, with {@code database} created on it. Versions run side by side,
 * each under its own host name; every class that asks for a version shares its server.
 */
public record MySQLSpec(MySQLVersion version, TestDatabaseName database)
    implements ContainerSpec<MySQLContainer> {

  /** Checks that both parts are set. */
  public MySQLSpec {
    Objects.requireNonNull(version, "version");
    Objects.requireNonNull(database, "database");
  }

  @Override
  public Class<MySQLContainer> containerType() {
    return MySQLContainer.class;
  }

  @Override
  public MySQLContainer getOrStart(ContainerSuite suite) {
    return switch (version) {
      case MYSQL_8_0 -> {
        suite.startMySQLContainer(database);
        yield suite.getMySQLContainer();
      }
      case MYSQL_5_7 -> {
        suite.startMySQLVersion5Container(database);
        yield suite.getMySQLVersion5Container();
      }
    };
  }

  static final class Factory implements ContainerFixture.SpecFactory<MySQLFixture> {
    @Override
    public Durability durabilityFor(MySQLFixture fixture) {
      return fixture.durability();
    }

    @Override
    public Startup startupFor(MySQLFixture fixture) {
      return fixture.startup();
    }

    @Override
    public MySQLSpec specFor(MySQLFixture fixture) {
      return new MySQLSpec(fixture.version(), fixture.database());
    }
  }
}
