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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.maintenance.jobs.BuiltInJobTemplateProvider;
import org.junit.jupiter.api.Test;

/** Tests orphan cleanup templates, SQL arguments, and location boundaries. */
public class TestIcebergRemoveOrphanFilesJob {
  /** Verifies template registration. */
  @Test
  public void testTemplateRegistration() {
    assertTrue(new IcebergRemoveOrphanFilesJob().jobTemplate().environments().isEmpty());
    assertTrue(
        new BuiltInJobTemplateProvider()
            .jobTemplates().stream()
                .anyMatch(t -> t.name().equals("builtin-iceberg-remove-orphan-files")));
    assertTrue(new IcebergRemoveOrphanFilesJob().jobTemplate().arguments().contains("{{dry_run}}"));
  }

  /** Verifies server-side template discovery without Spark, Iceberg, or Hadoop libraries. */
  @Test
  public void testTemplateLoadsWithoutSparkRuntime() throws Exception {
    List<URL> urls = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      File file = new File(entry);
      String name = file.getName();
      if (!name.startsWith("spark-")
          && !name.startsWith("iceberg-")
          && !name.startsWith("hadoop-")) {
        urls.add(file.toURI().toURL());
      }
    }
    try (URLClassLoader loader =
        new URLClassLoader(
            urls.toArray(new URL[0]), ClassLoader.getSystemClassLoader().getParent())) {
      assertThrows(
          ClassNotFoundException.class,
          () -> loader.loadClass("org.apache.spark.sql.SparkSession"));
      Class<?> providerClass = loader.loadClass(BuiltInJobTemplateProvider.class.getName());
      Object provider = providerClass.getDeclaredConstructor().newInstance();
      List<?> templates = (List<?>) providerClass.getMethod("jobTemplates").invoke(provider);
      List<String> names = new ArrayList<>();
      for (Object template : templates) {
        names.add((String) template.getClass().getMethod("name").invoke(template));
      }
      assertTrue(names.contains("builtin-iceberg-remove-orphan-files"));
    }
  }

  /** Verifies defaults and explicit dry run. */
  @Test
  public void testDefaultsAndExplicitDryRun() {
    assertEquals(
        "CALL `cat`.system.remove_orphan_files(table => 'db.tbl', dry_run => false)",
        IcebergRemoveOrphanFilesJob.buildProcedureCall("cat", "db.tbl", null, null, false));
    assertEquals(
        "CALL `cat`.system.remove_orphan_files(table => 'db.tbl', older_than => TIMESTAMP '2024-01-01 00:00:00', location => '/warehouse/tbl/data', dry_run => true)",
        IcebergRemoveOrphanFilesJob.buildProcedureCall(
            "cat", "db.tbl", "2024-01-01 00:00:00", "/warehouse/tbl/data", true));
    assertFalse(IcebergRemoveOrphanFilesJob.parseDryRun(null));
    assertFalse(IcebergRemoveOrphanFilesJob.parseDryRun("false"));
    assertTrue(IcebergRemoveOrphanFilesJob.parseDryRun("true"));
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesJob.parseDryRun("yes"));
  }

  /** Verifies sql escaping. */
  @Test
  public void testSqlEscaping() {
    String sql =
        IcebergRemoveOrphanFilesJob.buildProcedureCall("ca`t", "db.ta'ble", null, "/a/b'c", true);
    assertTrue(sql.contains("`ca``t`"));
    assertTrue(sql.contains("db.ta\\'ble"));
    assertTrue(sql.contains("/a/b\\'c"));
  }

  /** Verifies location boundaries. */
  @Test
  public void testLocationBoundaries() {
    assertDoesNotThrow(
        () ->
            IcebergRemoveOrphanFilesJob.validateLocation(
                "s3://bucket/db/table", "s3://bucket/db/table/data/"));
    assertDoesNotThrow(
        () ->
            IcebergRemoveOrphanFilesJob.validateLocation(
                "s3://bucket/db/table/", "s3://bucket/db/table"));
    for (String location :
        new String[] {
          "s3://bucket/db/table2",
          "s3://other/db/table",
          "s3://bucket/db",
          "s3://bucket/db/table/../other",
          "s3://bucket/db/table/%2e%2e/other",
          "s3://bucket/db/table?x=1",
          "relative/path",
          "s3a://bucket/db/table"
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () -> IcebergRemoveOrphanFilesJob.validateLocation("s3://bucket/db/table", location),
          location);
    }
  }
}
