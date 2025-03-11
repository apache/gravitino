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

package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonMultipleJDBCLoadIT extends BaseIT {
  private static final TestDatabaseName TEST_DB_NAME =
      TestDatabaseName.PG_TEST_PAIMON_CATALOG_MULTIPLE_JDBC_LOAD;

  private static MySQLContainer mySQLContainer;
  private static PostgreSQLContainer postgreSQLContainer;

  @BeforeAll
  public void startup() throws IOException {
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mySQLContainer = containerSuite.getMySQLContainer();
    containerSuite.startPostgreSQLContainer(TEST_DB_NAME);
    postgreSQLContainer = containerSuite.getPostgreSQLContainer();
  }

  @Test
  public void testCreateMultipleJdbcInPaimon() throws SQLException {
    String metalakeName = RandomNameUtils.genRandomName("it_metalake");
    String postgreSqlCatalogName = RandomNameUtils.genRandomName("it_paimon_postgresql");
    GravitinoMetalake metalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());

    Map<String, String> paimonPgConf = Maps.newHashMap();
    String jdbcUrl = postgreSQLContainer.getJdbcUrl(TEST_DB_NAME);
    paimonPgConf.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "jdbc");
    paimonPgConf.put(PaimonCatalogPropertiesMetadata.URI, jdbcUrl);
    paimonPgConf.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, "file:///tmp/paimon-pg-warehouse");
    paimonPgConf.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_USER, postgreSQLContainer.getUsername());
    paimonPgConf.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD, postgreSQLContainer.getPassword());
    paimonPgConf.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_DRIVER,
        postgreSQLContainer.getDriverClassName(TEST_DB_NAME));

    Catalog postgreSqlCatalog =
        metalake.createCatalog(
            postgreSqlCatalogName,
            Catalog.Type.RELATIONAL,
            "lakehouse-paimon",
            "comment",
            paimonPgConf);

    Map<String, String> paimonMysqlConf = Maps.newHashMap();

    paimonMysqlConf.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "jdbc");
    paimonMysqlConf.put(
        PaimonCatalogPropertiesMetadata.URI, mySQLContainer.getJdbcUrl(TEST_DB_NAME));
    paimonMysqlConf.put(
        PaimonCatalogPropertiesMetadata.WAREHOUSE, "file:///tmp/paimon-mysql-warehouse");
    paimonMysqlConf.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_USER, mySQLContainer.getUsername());
    paimonMysqlConf.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD, mySQLContainer.getPassword());
    paimonMysqlConf.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_DRIVER,
        mySQLContainer.getDriverClassName(TEST_DB_NAME));
    String mysqlCatalogName = RandomNameUtils.genRandomName("it_paimon_mysql");
    Catalog mysqlCatalog =
        metalake.createCatalog(
            mysqlCatalogName,
            Catalog.Type.RELATIONAL,
            "lakehouse-paimon",
            "comment",
            paimonMysqlConf);

    String[] nameIdentifiers = mysqlCatalog.asSchemas().listSchemas();
    Assertions.assertEquals(0, nameIdentifiers.length);
    nameIdentifiers = postgreSqlCatalog.asSchemas().listSchemas();
    Assertions.assertEquals(0, nameIdentifiers.length);

    String schemaName = RandomNameUtils.genRandomName("it_schema");
    mysqlCatalog.asSchemas().createSchema(schemaName, null, Collections.emptyMap());

    postgreSqlCatalog.asSchemas().createSchema(schemaName, null, Collections.emptyMap());

    String tableName = RandomNameUtils.genRandomName("it_table");

    Column col1 = Column.of("col_1", Types.IntegerType.get(), "col_1_comment");
    String comment = "test";
    mysqlCatalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            new Column[] {col1},
            comment,
            Collections.emptyMap());

    postgreSqlCatalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            new Column[] {col1},
            comment,
            Collections.emptyMap());

    Assertions.assertTrue(
        mysqlCatalog.asTableCatalog().tableExists(NameIdentifier.of(schemaName, tableName)));
    Assertions.assertTrue(
        postgreSqlCatalog.asTableCatalog().tableExists(NameIdentifier.of(schemaName, tableName)));
  }
}
