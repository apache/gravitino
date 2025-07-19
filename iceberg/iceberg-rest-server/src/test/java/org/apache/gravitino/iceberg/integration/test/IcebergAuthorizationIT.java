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
import com.google.errorprone.annotations.FormatMethod;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.integration.test.util.IcebergRESTServerManager;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.server.web.JettyServerConfig;
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
 * This is starts a Gravitino server to create metalake, catalogs and starts a standalone Iceberg
 * REST server to do the Authorization. There are two users: SUPER_USER is the owner of metalake,
 * has all privileges while NORMAL_USER is the user for Spark SQL, doesn't have privileges by
 * default. You could use Spark SQL to try to do the operation and use Gravitino client to grant
 * privilege or verify the Spark SQL result.
 */
public class IcebergAuthorizationIT extends BaseIT {

  private static final String GRAVITINO_ICEBERG_REST_PREFIX = "gravitino.iceberg-rest.";
  protected static final String METALAKE_NAME = "test_metalake";
  protected static final String CATALOG_NAME = "iceberg";

  protected static final String SUPER_USER = "super";

  // Iceberg doesn't support passing simple username for low version
  // todo: use a valid username after using Iceberg 1.9
  protected static final String NORMAL_USER = AuthConstants.ANONYMOUS_USER;

  protected Catalog catalogClientWithAllPrivilege;
  protected GravitinoMetalake metalakeClientWithAllPrivilege;

  private SparkSession sparkSession;
  private IcebergRESTServerManager icebergRESTServerManager;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergAuthorizationIT.class);

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    startGravitinoServerWithoutIcebergREST();
    initMetalakeAndCatalog();
    startStandAloneIcebergRESTServer();
    initSparkEnv();
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    client.dropMetalake(METALAKE_NAME, true);

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
    super.stopIntegrationTest();
  }

  private void initMetalakeAndCatalog() {
    metalakeClientWithAllPrivilege = client.createMetalake(METALAKE_NAME, "", new HashMap<>());
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

    catalogClientWithAllPrivilege =
        metalakeClientWithAllPrivilege.createCatalog(
            CATALOG_NAME,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "comment",
            getJDBCCatalogBackendConfig());
  }

  private void startGravitinoServerWithoutIcebergREST() throws Exception {
    setEntityStoreBackend("PostgreSQL");
    // Enable authorization
    customConfigs.putAll(
        ImmutableMap.of(
            "gravitino.authorization.serviceAdmins",
            SUPER_USER,
            "gravitino.authenticators",
            "simple",
            "SimpleAuthUserName",
            SUPER_USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.AUTHORIZATION_IMPL.getKey(),
            JcasbinAuthorizer.class.getCanonicalName(),
            Configs.CACHE_ENABLED.getKey(),
            "false",
            AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
                + AuxiliaryServiceManager.AUX_SERVICE_NAMES,
            ""));
    super.startIntegrationTest();
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
        MetadataObjects.of(Arrays.asList(CATALOG_NAME), MetadataObject.Type.CATALOG);
    metalakeClientWithAllPrivilege.setOwner(catalogObject, SUPER_USER, Owner.Type.USER);
  }

  private void startStandAloneIcebergRESTServer() throws Exception {
    icebergRESTServerManager = IcebergRESTServerManager.create();
    icebergRESTServerManager.registerCustomConfigs(getIcebergCatalogConfig());
    icebergRESTServerManager.startIcebergRESTServer();
  }

  private int getServerPort() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(
            icebergRESTServerManager.getServerConfig(), IcebergConfig.ICEBERG_CONFIG_PREFIX);
    return jettyServerConfig.getHttpPort();
  }

  private void initSparkEnv() {
    String icebergRESTUri = String.format("http://127.0.0.1:%d/iceberg/", getServerPort());
    LOG.info("Iceberg REST uri: {}", icebergRESTUri);
    SparkConf sparkConf =
        new SparkConf()
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.rest.type", "rest")
            .set("spark.sql.catalog.rest.prefix", CATALOG_NAME)
            .set("spark.sql.catalog.rest.uri", icebergRESTUri)
            .set("spark.sql.catalog.rest.rest.auth.type", "basic")
            .set("spark.sql.catalog.rest.rest.auth.basic.username", NORMAL_USER)
            .set("spark.sql.catalog.rest.rest.auth.basic.password", "mock")
            // drop Iceberg table purge may hang in spark local mode
            .set("spark.locality.wait.node", "0");

    sparkSession = SparkSession.builder().master("local[1]").config(sparkConf).getOrCreate();
  }

  private Map<String, String> getIcebergCatalogConfig() {
    // add entity store configurations
    Map<String, String> configs = new HashMap<>(customConfigs);
    // add jdbc catalog backend configs
    getJDBCCatalogBackendConfig()
        .forEach((k, v) -> configs.put(GRAVITINO_ICEBERG_REST_PREFIX + k, v));
    // add dynamic config provider configs
    configs.putAll(
        ImmutableMap.of(
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER,
            IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME,
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_URI,
            serverUri,
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_METALAKE,
            METALAKE_NAME,
            GRAVITINO_ICEBERG_REST_PREFIX + IcebergConstants.GRAVITINO_SIMPLE_USERNAME,
            SUPER_USER));
    return configs;
  }

  private Map<String, String> getJDBCCatalogBackendConfig() {
    return ImmutableMap.of(
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
        IcebergConstants.WAREHOUSE,
        "file:///tmp/");
  }

  private String getPGUri() {
    return customConfigs.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY);
  }

  private String getPGUser() {
    return customConfigs.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY);
  }

  private String getPGPassword() {
    return customConfigs.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY);
  }

  protected Map<String, String> etTableInfo(String tableName) {
    return convertToStringMap(sql("desc table extended %s", tableName));
  }

  @FormatMethod
  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = sparkSession.sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }
    return rowsToJava(rows);
  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
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
