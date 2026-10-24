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

import java.util.stream.Stream;
import org.apache.gravitino.integration.test.util.CloseContainerExtension;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Starts a durable private container beside the shared one for each service that starts in a minute
 * or two. The heavier services (Doris, OceanBase, Ranger, StarRocks) share the same start path,
 * which {@link TestPrivateScope} checks without Docker.
 */
@Tag("gravitino-docker-test")
@ExtendWith(CloseContainerExtension.class)
public class TestPrivateContainers {

  static Stream<ContainerSpec<?>> specs() {
    return Stream.of(
        new MySQLSpec(MySQLVersion.MYSQL_8_0, TestDatabaseName.MYSQL_CATALOG_MYSQL_IT),
        new PostgreSQLSpec(PGImageName.VERSION_13, TestDatabaseName.PG_CATALOG_POSTGRESQL_IT),
        new ClickHouseSpec(1, TestDatabaseName.CLICKHOUSE_CATALOG_CLICKHOUSE_IT),
        new ZooKeeperSpec(),
        new KafkaSpec(),
        new LocalStackSpec(),
        new MotoSpec(),
        new RustFSSpec(),
        new HiveSpec(HiveVersion.HIVE2, HiveAuth.NONE));
  }

  @ParameterizedTest
  @MethodSource("specs")
  void testPrivateContainerRunsBesideTheSharedOne(ContainerSpec<?> spec) {
    BaseContainer shared = Fixtures.get(spec);

    try (BaseContainer own =
        spec.startPrivate(SharedContainers.getInstance(), Durability.DURABLE)) {
      Assertions.assertNotSame(shared, own);
      Assertions.assertEquals(Durability.DURABLE, own.durability());
      Assertions.assertTrue(own.getContainer().isRunning());
      Assertions.assertNotEquals(shared.getContainerIpAddress(), own.getContainerIpAddress());
    }

    Assertions.assertSame(shared, Fixtures.get(spec));
    Assertions.assertTrue(shared.getContainer().isRunning());
  }
}
