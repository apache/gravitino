/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.integration.test;

import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.flink.connector.PropertiesConverter;
import com.datastrato.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import java.util.Collections;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlinkEnvIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvIT.class);
  protected static final String metalakeName = "flink";

  protected static GravitinoMetalake metalake;
  protected static TableEnvironment tableEnv;

  private static String metalakeUri = "http://127.0.0.1:8090";

  @BeforeAll
  static void startUp() throws Exception {
    // Start Gravitino server
    initGravitinoEnv();
    initMetalake();
    initFlinkEnv();
    LOG.info("Startup Flink env successfully, metalake uri: {}.", metalakeUri);
  }

  @AfterAll
  static void stop() {}

  protected String flinkByPass(String key) {
    return PropertiesConverter.FLINK_PROPERTY_PREFIX + key;
  }

  private static void initGravitinoEnv() {
    // Gravitino server is already started by AbstractIT, just construct gravitinoUrl
    int gravitinoPort = getGravitinoServerPort();
    metalakeUri = String.format("http://127.0.0.1:%d", gravitinoPort);
  }

  private static void initMetalake() {
    metalake = client.createMetalake(metalakeName, "", Collections.emptyMap());
  }

  private static void initFlinkEnv() {
    final Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.metalake.name", metalakeName);
    configuration.setString("table.catalog-store.gravitino.metalake.uri", metalakeUri);
    tableEnv = TableEnvironment.create(configuration);
  }
}
