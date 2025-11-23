/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.FormatMethod;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.iceberg.CatalogProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/**
 * This IT starts a Gravitino server with Iceberg REST server in the auxiliary mode, we could create
 * metalake, catalogs in Gravitino server and do the authorization with Iceberg REST server. There
 * are two users: SUPER_USER is the owner of metalake, has all privileges while NORMAL_USER is the
 * user for Spark SQL, doesn't have privileges by default. You could use Spark SQL to try to do the
 * operation and use Gravitino client to grant privilege or check the test result.
 */
public class IcebergAuthorizationIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergAuthorizationIT.class);
  private static final String GRAVITINO_ICEBERG_REST_PREFIX = "gravitino.iceberg-rest.";
  protected static final String METALAKE_NAME = "test_metalake";
  protected static final String GRAVITINO_CATALOG_NAME = "iceberg";
  protected static final String SPARK_CATALOG_NAME = "rest";

  protected static final String SUPER_USER = "super";
  protected static final String NORMAL_USER = "normal";

  protected Catalog catalogClientWithAllPrivilege;
  protected GravitinoMetalake metalakeClientWithAllPrivilege;

  private static ContainerSuite containerSuite = ContainerSuite.getInstance();
  private SparkSession sparkSession;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    containerSuite.startPostgreSQLContainer(TestDatabaseName.PG_ICEBERG_AUTHZ_IT);
    startGravitinoServerWithIcebergREST();
    initMetalakeAndCatalog();
    initSparkEnv();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    sparkSession.close();
    client.dropMetalake(METALAKE_NAME, true);

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
    super.stopIntegrationTest();
  }

  private void initMetalakeAndCatalog() {
    this.metalakeClientWithAllPrivilege = client.createMetalake(METALAKE_NAME, "", new HashMap<>());
    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(Arrays.asList(METALAKE_NAME), MetadataObject.Type.METALAKE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(SUPER_USER, owner.get().name());

    try {
      metalakeClientWithAllPrivilege.getUser(NORMAL_USER);
    } catch (NoSuchUserException e) {
      metalakeClientWithAllPrivilege.addUser(NORMAL_USER);
    }

    Map<String, String> basicProps =
        ImmutableMap.of(
            IcebergConstants.URI,
            getPGUri(),
            IcebergConstants.CATALOG_BACKEND,
            "jdbc",
            IcebergConstants.GRAVITINO_JDBC_DRIVER,
            "org.postgresql.Driver",
            IcebergConstants.GRAVITINO_JDBC_USER,
            getPGUser(),
            IcebergConstants.GRAVITINO_JDBC_PASSWORD,
            getPGPassword(),
            "gravitino.bypass.jdbc.schema-version",
            "v1",
            IcebergConstants.ICEBERG_JDBC_INITIALIZE,
            "true",
            IcebergConstants.WAREHOUSE,
            "file:///tmp/");

    Map<String, String> customProps = getCustomProperties();
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.putAll(basicProps);
    catalogProps.putAll(customProps);

    catalogClientWithAllPrivilege =
        metalakeClientWithAllPrivilege.createCatalog(
            GRAVITINO_CATALOG_NAME,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "comment",
            catalogProps);
  }

  private void startGravitinoServerWithIcebergREST() throws Exception {
    ignoreIcebergAuxRestService = false;
    // Enable authorization
    customConfigs.putAll(
        ImmutableMap.of(
            "gravitino.authorization.serviceAdmins",
            SUPER_USER,
            "gravitino.authenticators",
            "simple",
            // BaseIT will use SimpleAuthUserName to create Gravitino client
            "SimpleAuthUserName",
            SUPER_USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.AUTHORIZATION_IMPL.getKey(),
            JcasbinAuthorizer.class.getCanonicalName(),
            Configs.CACHE_ENABLED.getKey(),
            "true"));
    // add dynamic config provider configs
    customConfigs.putAll(
        ImmutableMap.of(
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
            IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE,
            METALAKE_NAME,
            GRAVITINO_ICEBERG_REST_PREFIX
                + IcebergConstants.ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME,
            GRAVITINO_CATALOG_NAME,
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_SIMPLE_USERNAME,
            SUPER_USER));
    super.startIntegrationTest();
  }

  protected Map<String, String> getCustomProperties() {
    return Maps.newHashMap();
  }

  protected boolean supportsCredentialVending() {
    return false;
  }

  void revokeUserRoles() {
    List<String> roles = metalakeClientWithAllPrivilege.getUser(NORMAL_USER).roles();
    if (roles.size() > 0) {
      metalakeClientWithAllPrivilege.revokeRolesFromUser(roles, NORMAL_USER);
    }
    roles = metalakeClientWithAllPrivilege.getUser(NORMAL_USER).roles();
    Assertions.assertEquals(0, roles.size());
  }

  void resetMetalakeAndCatalogOwner() {
    metalakeClientWithAllPrivilege.setOwner(
        MetadataObjects.of(Arrays.asList(METALAKE_NAME), MetadataObject.Type.METALAKE),
        SUPER_USER,
        Owner.Type.USER);
    MetadataObject catalogObject =
        MetadataObjects.of(Arrays.asList(GRAVITINO_CATALOG_NAME), MetadataObject.Type.CATALOG);
    metalakeClientWithAllPrivilege.setOwner(catalogObject, SUPER_USER, Owner.Type.USER);
  }

  void createTable(String schemaName, String tableName) {
    Column col1 = Column.of("col_1", Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of("col_2", Types.IntegerType.get(), "col_2_comment");
    catalogClientWithAllPrivilege
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            new Column[] {col1, col2},
            "table_comment",
            new HashMap<>());
    boolean exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(schemaName, tableName));
    Assertions.assertTrue(exists);
  }

  private void initSparkEnv() {
    String icebergRESTUri = getIcebergRestServiceUri();
    LOG.info("Iceberg REST uri: {}", icebergRESTUri);
    SparkConf sparkConf =
        new SparkConf()
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.rest.type", "rest")
            .set("spark.sql.catalog.rest.uri", icebergRESTUri)
            // disable spark side table cache to check the privilege in each operation
            .set("spark.sql.catalog.rest." + CatalogProperties.CACHE_ENABLED, "false")
            .set("spark.sql.catalog.rest.rest.auth.type", "basic")
            .set("spark.sql.catalog.rest.rest.auth.basic.username", NORMAL_USER)
            .set("spark.sql.catalog.rest.rest.auth.basic.password", "mock")
            // drop Iceberg table purge may hang in spark local mode
            .set("spark.locality.wait.node", "0");
    if (supportsCredentialVending()) {
      sparkConf.set(
          "spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation", "vended-credentials");
    }

    sparkSession = SparkSession.builder().master("local[1]").config(sparkConf).getOrCreate();
  }

  private String getPGUri() {
    return containerSuite.getPostgreSQLContainer().getJdbcUrl(TestDatabaseName.PG_ICEBERG_AUTHZ_IT);
  }

  private String getPGUser() {
    return containerSuite.getPostgreSQLContainer().getUsername();
  }

  private String getPGPassword() {
    return containerSuite.getPostgreSQLContainer().getPassword();
  }

  protected Map<String, String> getTableInfo(String tableName) {
    return convertToStringMap(sql("desc table extended %s", tableName));
  }

  protected Set<String> listTableNames(String database) {
    return convertToStringSet(sql("SHOW TABLES in %s", database), 1);
  }

  protected List<Object[]> sql2(String query) {
    List<Row> rows = sparkSession.sql(query).collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }
    return rowsToJava(rows);
  }

  @FormatMethod
  protected List<Object[]> sql(String query, Object... args) {
    return sql2(String.format(query, args));
  }

  private static Set<String> convertToStringSet(List<Object[]> objects, int index) {
    return objects.stream().map(row -> String.valueOf(row[index])).collect(Collectors.toSet());
  }

  private List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }
              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              }
              return value;
            })
        .toArray(Object[]::new);
  }

  private static Map<String, String> convertToStringMap(List<Object[]> objects) {
    return objects.stream()
        .collect(
            Collectors.toMap(
                row -> String.valueOf(row[0]),
                row -> String.valueOf(row[1]),
                (oldValue, newValue) -> oldValue));
  }
}
