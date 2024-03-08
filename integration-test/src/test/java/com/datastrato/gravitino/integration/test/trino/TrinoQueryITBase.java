/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import static java.lang.Thread.sleep;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.exceptions.RESTException;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.TrinoITContainers;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.TableCatalog;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

public class TrinoQueryITBase {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoQueryITBase.class);

  // Auto start docker containers and gravitino server
  protected static boolean autoStart = true;

  // Auto start gravitino server
  protected static boolean autoStartGravitino = true;

  protected static boolean started = false;

  // TODO(yuhui) redo get the configs after we have the Docker image ready for testing.
  protected static String gravitinoUri = "http://127.0.0.1:8090";
  protected static String trinoUri = "http://127.0.0.1:8080";
  protected static String hiveMetastoreUri = "thrift://127.0.0.1:9083";
  protected static String hdfsUri = "hdfs://127.0.0.1:9000";
  protected static String mysqlUri = "jdbc:mysql://127.0.0.1";
  protected static String postgresqlUri = "jdbc:postgresql://127.0.0.1";

  protected static GravitinoClient gravitinoClient;
  protected static TrinoITContainers trinoITContainers;
  protected static TrinoQueryRunner trinoQueryRunner;

  protected static final String metalakeName = "test";
  protected static GravitinoMetaLake metalake;

  private static void setEnv() throws Exception {
    if (autoStart) {
      AbstractIT.startIntegrationTest();
      gravitinoClient = AbstractIT.getGravitinoClient();
      gravitinoUri = String.format("http://127.0.0.1:%d", AbstractIT.getGravitinoServerPort());

      trinoITContainers = ContainerSuite.getTrinoITContainers();
      trinoITContainers.launch(AbstractIT.getGravitinoServerPort());

      trinoUri = trinoITContainers.getTrinoUri();
      hiveMetastoreUri = trinoITContainers.getHiveMetastoreUri();
      hdfsUri = trinoITContainers.getHdfsUri();
      mysqlUri = trinoITContainers.getMysqlUri();
      postgresqlUri = trinoITContainers.getPostgresqlUri();

    } else if (autoStartGravitino) {
      AbstractIT.startIntegrationTest();
      gravitinoClient = AbstractIT.getGravitinoClient();
      gravitinoUri = String.format("http://127.0.0.1:%d", AbstractIT.getGravitinoServerPort());

    } else {
      gravitinoClient = GravitinoClient.builder(gravitinoUri).build();
    }
  }

  public static void setup() throws Exception {
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
        AbstractIT.stopIntegrationTest();
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
        boolean exists = gravitinoClient.metalakeExists(NameIdentifier.of(metalakeName));

        if (exists) {
          metalake = gravitinoClient.loadMetalake(NameIdentifier.of(metalakeName));
          return;
        }

        GravitinoMetaLake createdMetalake =
            gravitinoClient.createMetalake(
                NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
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
    boolean exists = gravitinoClient.metalakeExists(NameIdentifier.of(metalakeName));
    if (!exists) {
      return;
    }
    gravitinoClient.dropMetalake(NameIdentifier.of(metalakeName));
  }

  private static void createCatalog(
      String catalogName, String provider, Map<String, String> properties) throws Exception {
    boolean exists = metalake.catalogExists(NameIdentifier.of(metalakeName, catalogName));
    if (!exists) {
      Catalog createdCatalog =
          metalake.createCatalog(
              NameIdentifier.of(metalakeName, catalogName),
              Catalog.Type.RELATIONAL,
              provider,
              "comment",
              properties);
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
    boolean exists = metalake.catalogExists(NameIdentifier.of(metalakeName, catalogName));
    if (!exists) {
      return;
    }
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas schemas = catalog.asSchemas();
    Arrays.stream(schemas.listSchemas(Namespace.ofSchema(metalakeName, catalogName)))
        .filter(schema -> !schema.name().equals("default") && schema.name().startsWith("gt_"))
        .forEach(
            schema -> {
              try {
                TableCatalog tableCatalog = catalog.asTableCatalog();
                Arrays.stream(
                        tableCatalog.listTables(
                            Namespace.ofTable(metalakeName, catalogName, schema.name())))
                    .forEach(
                        table -> {
                          boolean dropped =
                              tableCatalog.dropTable(
                                  NameIdentifier.ofTable(
                                      metalakeName, catalogName, schema.name(), table.name()));
                          LOG.info(
                              "Drop table \"{}.{}\".{}.{}",
                              metalakeName,
                              catalogName,
                              schema.name(),
                              table.name());
                          if (!dropped) {
                            LOG.error("Failed to drop table {}", table);
                          }
                        });

                schemas.dropSchema(
                    NameIdentifier.ofSchema(metalakeName, catalogName, schema.name()), false);
              } catch (Exception e) {
                LOG.error("Failed to drop schema {}", schema);
              }
              LOG.info("Drop schema \"{}.{}\".{}", metalakeName, catalogName, schema.name());
            });

    metalake.dropCatalog(NameIdentifier.of(metalakeName, catalogName));
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
    throw new Exception("Test queries directory does not exist");
  }
}
