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
package org.apache.gravitino.catalog.doris.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.DorisContainer;
import org.apache.gravitino.integration.test.container.DorisImageName;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Integration tests for Doris 2.1.0 with the Nereids planner enabled. */
@Tag("gravitino-docker-test")
@Tag("doris-multi-version")
public class CatalogDoris2xIT extends BaseIT {

  private static final String PROVIDER = "jdbc-doris";
  private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private final String metalakeName = GravitinoITUtils.genRandomName("doris2x_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("doris2x_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("doris2x_schema");

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private String jdbcUrl;

  @BeforeAll
  public void startup() {
    containerSuite.startDorisContainer(DorisImageName.VERSION_2_1);
    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);
  }

  @AfterEach
  public void resetSchema() {
    catalog.asSchemas().dropSchema(schemaName, true);
    createSchema();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"a real comment", "quote \" and apostrophe ' and backslash \\ and newline\nend"})
  void testTableCommentWithNereids(String comment) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl + schemaName, DorisContainer.USER_NAME, DorisContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      try (ResultSet result = statement.executeQuery("SELECT @@enable_nereids_planner")) {
        assertTrue(result.next());
        assertTrue(result.getBoolean(1));
      }

      TableCatalog tables = catalog.asTableCatalog();
      NameIdentifier identifier = NameIdentifier.of(schemaName, "comment_with_nereids");
      Table created =
          tables.createTable(
              identifier,
              new Column[] {Column.of("id", Types.IntegerType.get(), null, false, false, null)},
              comment,
              Collections.emptyMap(),
              Transforms.EMPTY_TRANSFORM,
              Distributions.hash(1, NamedReference.field("id")),
              null);
      assertEquals(comment, created.comment());
      assertEquals(comment, tables.loadTable(identifier).comment());

      try (PreparedStatement query =
          connection.prepareStatement(
              "SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")) {
        query.setString(1, schemaName);
        query.setString(2, identifier.name());
        try (ResultSet result = query.executeQuery()) {
          assertTrue(result.next());
          String storedComment = result.getString(1);
          assertNotNull(StringIdentifier.fromComment(storedComment));
          assertEquals(comment, StringIdentifier.removeIdFromComment(storedComment));
        }
      }

      try (ResultSet result = statement.executeQuery("SHOW CREATE TABLE `comment_with_nereids`")) {
        assertTrue(result.next());
        assertTrue(result.getString(2).contains("gravitino.v1.uid"));
      }
    }
  }

  private void createMetalake() {
    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);
    assertEquals(metalakeName, metalake.name());
  }

  private void createCatalog() {
    DorisContainer dorisContainer = containerSuite.getDorisContainer(DorisImageName.VERSION_2_1);
    jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/",
            dorisContainer.getContainerIpAddress(), dorisContainer.getFeMysqlPort());

    Map<String, String> props = Maps.newHashMap();
    props.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    props.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    props.put(JdbcConfig.USERNAME.getKey(), DorisContainer.USER_NAME);
    props.put(JdbcConfig.PASSWORD.getKey(), DorisContainer.PASSWORD);

    catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, PROVIDER, "doris 2.x catalog", props);
    assertEquals(catalogName, metalake.loadCatalog(catalogName).name());
  }

  private void createSchema() {
    catalog.asSchemas().createSchema(schemaName, null, Collections.emptyMap());
    assertEquals(schemaName, catalog.asSchemas().loadSchema(schemaName).name());
  }
}
