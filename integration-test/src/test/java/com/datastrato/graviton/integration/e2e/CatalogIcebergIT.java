/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.e2e;

import com.datastrato.graviton.server.GravitonServer;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogIcebergIT extends CatalogHiveIT {

  private File confFile;

  private List<String> oldFileLines;

  @BeforeAll
  @Override
  public void startup() throws Exception {
    String confDir =
        Optional.ofNullable(System.getenv("GRAVITON_CONF_DIR"))
            .orElse(
                Optional.ofNullable(System.getenv("GRAVITON_HOME"))
                    .map(s -> s + File.separator + "conf")
                    .orElse(null));

    if (confDir == null) {
      throw new IllegalArgumentException("GRAVITON_CONF_DIR or GRAVITON_HOME not set");
    }

    confFile = new File(confDir + File.separator + GravitonServer.CONF_FILE);
    oldFileLines = FileUtils.readLines(confFile);
    FileUtils.writeLines(
        confFile,
        Collections.singletonList(
            "graviton.server.auxService.GravitonIcebergREST.CatalogImpl = hive"));

    super.startup();
  }

  @AfterAll
  @Override
  public void stop() {
    if (null != oldFileLines) {
      FileUtils.deleteQuietly(confFile);
      try {
        confFile.createNewFile();
        FileUtils.writeLines(confFile, oldFileLines);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    super.stop();
  }

  @Test
  @Override
  public void testCreateHiveTable() throws TException, InterruptedException {
    super.testCreateHiveTable();
  }

  @Test
  @Override
  public void testCreatePartitionedHiveTable() throws TException, InterruptedException {
    super.testCreatePartitionedHiveTable();
  }

  @Test
  @Override
  public void testAlterHiveTable() throws TException, InterruptedException {
    super.testAlterHiveTable();
  }

  @Test
  @Override
  public void testDropHiveTable() {
    super.testDropHiveTable();
  }

  @Test
  @Override
  public void testAlterSchema() throws TException, InterruptedException {
    super.testAlterSchema();
  }

  @Override
  protected String getProvider() {
    return "iceberg";
  }
}
