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
package org.apache.gravitino.trino.connector.integration.test;

import static java.lang.Thread.sleep;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.TrinoITContainers;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

public class TrinoQueryITBase {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoQueryITBase.class);

  // Auto start docker containers and Gravitino server
  protected static boolean autoStart = true;

  // Auto start Gravitino server
  protected static boolean autoStartGravitino = true;

  protected static boolean started = false;

  public static String testHost = "127.0.0.1";
  public static String gravitinoUri = String.format("http://%s:8090", testHost);
  public static String trinoUri = String.format("http://%s:8080", testHost);
  public static String hiveMetastoreUri = String.format("thrift://%s:9083", testHost);
  public static String hdfsUri = String.format("hdfs://%s:9000", testHost);
  public static String mysqlUri = String.format("jdbc:mysql://%s", testHost);
  public static String postgresqlUri = String.format("jdbc:postgresql://%s", testHost);

  protected static GravitinoAdminClient gravitinoClient;
  protected static TrinoITContainers trinoITContainers;
  protected static TrinoQueryRunner trinoQueryRunner;

  protected static final String metalakeName = "test";
  protected static GravitinoMetalake metalake;

  private static BaseIT baseIT;

  protected int trinoWorkerNum = 0;

  public TrinoQueryITBase() {}

  public TrinoQueryITBase(int trinoWorkerNum) {
    this.trinoWorkerNum = trinoWorkerNum;
  }

  private void setEnv() throws Exception {
    baseIT = new BaseIT();
    if (autoStart) {
      baseIT.startIntegrationTest();
      gravitinoClient = baseIT.getGravitinoClient();
      gravitinoUri = String.format("http://127.0.0.1:%d", baseIT.getGravitinoServerPort());

      trinoITContainers = ContainerSuite.getTrinoITContainers();

      /**
       * When connecting to a Hive metastore version 3.x, the Hive connector supports reading from
       * and writing to insert-only and ACID tables, with full support for partitioning and
       * bucketing
       */
      String hiveRuntimeVersion = "hive3";

      /**
       * Only Trino connector test will create Hive ACID table when deploy Hive with
       * `/integration-test-common/docker-script/init/hive/init.sh`
       */
      Boolean isTrinoConnectorTest = true;

      trinoITContainers.launch(
          baseIT.getGravitinoServerPort(),
          hiveRuntimeVersion,
          isTrinoConnectorTest,
          trinoWorkerNum);

      trinoUri = trinoITContainers.getTrinoUri();
      hiveMetastoreUri = trinoITContainers.getHiveMetastoreUri();
      hdfsUri = trinoITContainers.getHdfsUri();
      mysqlUri = trinoITContainers.getMysqlUri();
      postgresqlUri = trinoITContainers.getPostgresqlUri();

    } else if (autoStartGravitino) {
      baseIT.startIntegrationTest();
      gravitinoClient = baseIT.getGravitinoClient();
      gravitinoUri = String.format("http://127.0.0.1:%d", baseIT.getGravitinoServerPort());

    } else {
      gravitinoClient = GravitinoAdminClient.builder(gravitinoUri).build();
    }
  }

  public void setup() throws Exception {
    if (started) {
      return;
    }

    setEnv();
    trinoQueryRunner = new TrinoQueryRunner(trinoUri);
    createMetalake();

    started = true;
  }

  public static void cleanup() {
    if (trinoQueryRunner != null) {
      trinoQueryRunner.stop();
    }

    try {
      if (autoStart) {
        if (trinoITContainers != null) trinoITContainers.shutdown();
        baseIT.stopIntegrationTest();
      }
    } catch (Exception e) {
      LOG.error("Error in cleanup", e);
    }
  }

  private static void createMetalake() throws Exception {
    boolean created = false;
    int tries = 180;
    while (!created && tries-- >= 0) {
      try {
        boolean exists = gravitinoClient.metalakeExists(metalakeName);

        if (exists) {
          metalake = gravitinoClient.loadMetalake(metalakeName);
          return;
        }

        GravitinoMetalake createdMetalake =
            gravitinoClient.createMetalake(metalakeName, "comment", Collections.emptyMap());
        Assertions.assertNotNull(createdMetalake);
        metalake = createdMetalake;
        created = true;
      } catch (RESTException exception) {
        LOG.info("Waiting for connecting to gravitino server");
        sleep(1000);
      }
    }

    if (!created) {
      throw new Exception("Failed to create metalake " + metalakeName);
    }
  }

  private static void dropMetalake() {
    boolean exists = gravitinoClient.metalakeExists(metalakeName);
    if (!exists) {
      return;
    }
    gravitinoClient.dropMetalake(metalakeName, true);
  }

  private static void createCatalog(
      String catalogName, String provider, Map<String, String> properties) throws Exception {
    boolean exists = metalake.catalogExists(catalogName);
    if (!exists) {
      Catalog createdCatalog =
          metalake.createCatalog(
              catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
      Assertions.assertNotNull(createdCatalog);
    }

    boolean catalogCreated = false;
    int tries = 180;
    while (!catalogCreated && tries-- >= 0) {
      try {
        String result = trinoQueryRunner.runQuery("show catalogs");
        if (result.contains(metalakeName + "." + catalogName)) {
          catalogCreated = true;
          break;
        }
        LOG.info("Waiting for catalog {} to be created", catalogName);
        // connection exception need retry.
      } catch (Exception ConnectionException) {
        LOG.info("Waiting for connecting to Trino");
      }
      sleep(1000);
    }

    if (!catalogCreated) {
      throw new Exception("Catalog " + catalogName + " create timeout");
    }
  }

  protected static void dropCatalog(String catalogName) {
    if (metalake == null) {
      return;
    }
    boolean exists = metalake.catalogExists(catalogName);
    if (!exists) {
      return;
    }
    Catalog catalog = metalake.loadCatalog(catalogName);
    SupportsSchemas schemas = catalog.asSchemas();
    Arrays.stream(schemas.listSchemas())
        .filter(schema -> schema.startsWith("gt_"))
        .forEach(
            schema -> {
              try {
                TableCatalog tableCatalog = catalog.asTableCatalog();
                Arrays.stream(tableCatalog.listTables(Namespace.of(schema)))
                    .forEach(
                        table -> {
                          boolean dropped =
                              tableCatalog.dropTable(NameIdentifier.of(schema, table.name()));
                          LOG.info(
                              "Drop table \"{}.{}\".{}.{}",
                              metalakeName,
                              catalogName,
                              schema,
                              table.name());
                          if (!dropped) {
                            LOG.error("Failed to drop table {}", table);
                          }
                        });

                schemas.dropSchema(schema, false);
              } catch (Exception e) {
                LOG.error("Failed to drop schema {}", schema, e);
              }
              LOG.info("Drop schema \"{}.{}\".{}", metalakeName, catalogName, schema);
            });

    metalake.disableCatalog(catalogName);
    metalake.dropCatalog(catalogName, true);
    LOG.info("Drop catalog \"{}.{}\"", metalakeName, catalogName);
  }

  public static String readFileToString(String filename) throws IOException {
    try {
      return FileUtils.readFileToString(new File(filename), StandardCharsets.UTF_8) + "\n\n";
    } catch (Exception e) {
      throw new IOException("Failed to read test file " + filename);
    }
  }

  public static String[] listDirectory(String dirname) throws Exception {
    File dir = new File(dirname);
    if (dir.exists()) {
      return dir.list();
    }
    throw new Exception("Test queries directory " + dirname + " does not exist");
  }
}
