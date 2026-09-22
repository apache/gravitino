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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises the job entry point against Spark 3.5 and Iceberg 1.11.0. */
public class TestIcebergRewriteManifestsJobWithSpark {
  private static final String TABLE = "manifest_catalog.db.events";

  @TempDir Path temporaryDirectory;
  private SparkSession spark;

  /** Creates multiple manifests for both day and hour partition specs. */
  @BeforeEach
  public void setUp() {
    startSpark();
    spark.sql("CREATE NAMESPACE manifest_catalog.db");
    spark.sql(
        "CREATE TABLE "
            + TABLE
            + " (id INT, event_time TIMESTAMP) USING iceberg "
            + "PARTITIONED BY (days(event_time)) "
            + "TBLPROPERTIES ('commit.manifest-merge.enabled'='false')");
    insertRows(0);
    spark.sql(
        "ALTER TABLE "
            + TABLE
            + " REPLACE PARTITION FIELD days(event_time) WITH hours(event_time)");
    insertRows(3);
  }

  /** Releases Spark resources after each test. */
  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  /** Omitting spec_id selects the current spec and leaves old-spec manifests untouched. */
  @Test
  public void testDefaultSpecAndNoOpPreserveData() {
    List<Row> records = records();
    Set<String> oldManifests = manifests(0);
    assertEquals(3, oldManifests.size());
    assertEquals(3, manifests(1).size());
    runJob(null, null);
    assertEquals(oldManifests, manifests(0));
    assertEquals(1, manifests(1).size());
    assertEquals(records, records());
    Set<String> allManifests = manifests(1);
    long snapshot = currentSnapshot();
    runJob(null, "false");
    assertEquals(allManifests, manifests(1));
    assertEquals(snapshot, currentSnapshot());
    assertEquals(records, records());
  }

  /** An explicit old spec is rewritten without migrating it or changing the current spec. */
  @Test
  public void testExplicitOldSpecWithCaching() {
    List<Row> records = records();
    Set<String> currentManifests = manifests(1);
    assertEquals(3, manifests(0).size());
    runJob("0", "true");
    assertEquals(1, manifests(0).size());
    assertEquals(currentManifests, manifests(1));
    assertEquals(records, records());
    // A subsequent default run still selects spec 1.
    Set<String> oldManifests = manifests(0);
    runJob(null, null);
    assertEquals(oldManifests, manifests(0));
    assertEquals(1, manifests(1).size());
  }

  /** A nonexistent spec fails the entry point, stops Spark, and leaves metadata unchanged. */
  @Test
  public void testUnknownSpecFailsAndStopsSpark() {
    long snapshot = currentSnapshot();
    Set<String> oldManifests = manifests(0);
    Set<String> newManifests = manifests(1);
    SparkSession running = spark;
    PrintStream originalErr = System.err;
    ByteArrayOutputStream errors = new ByteArrayOutputStream();
    try (PrintStream captured = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
      System.setErr(captured);
      assertEquals(1, IcebergRewriteManifestsJob.run(arguments("999", null)));
    } finally {
      System.setErr(originalErr);
    }
    String message = errors.toString(StandardCharsets.UTF_8);
    assertTrue(message.contains("Error rewriting manifests:"));
    assertTrue(message.contains("999"));
    assertFalse(
        message.contains(
            "\tat org.apache.gravitino.maintenance.jobs.iceberg.IcebergRewriteManifestsJob"));
    assertTrue(running.sparkContext().isStopped());
    startSpark();
    assertEquals(snapshot, currentSnapshot());
    assertEquals(oldManifests, manifests(0));
    assertEquals(newManifests, manifests(1));
    assertEquals(6, records().size());
  }

  private void runJob(String spec, String caching) {
    spark.stop();
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    PrintStream originalOut = System.out;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (PrintStream captured = new PrintStream(output, true, StandardCharsets.UTF_8)) {
      System.setOut(captured);
      IcebergRewriteManifestsJob.main(arguments(spec, caching));
    } finally {
      System.setOut(originalOut);
    }
    assertTrue(
        output
            .toString(StandardCharsets.UTF_8)
            .contains("Rewrite Manifests Results: Rewritten manifests:"));
    if (SparkSession.getActiveSession().isDefined()) {
      assertTrue(SparkSession.getActiveSession().get().sparkContext().isStopped());
    }
    if (SparkSession.getDefaultSession().isDefined()) {
      assertTrue(SparkSession.getDefaultSession().get().sparkContext().isStopped());
    }
    startSpark();
  }

  private String[] arguments(String spec, String caching) {
    Map<String, String> jobConf = new HashMap<>();
    jobConf.put("catalog_name", "manifest_catalog");
    jobConf.put("table_identifier", "db.events");
    jobConf.put(
        "spark_conf",
        "{\"spark.master\":\"local[2]\",\"spark.ui.enabled\":\"false\","
            + "\"spark.sql.shuffle.partitions\":\"2\","
            + "\"spark.sql.extensions\":\"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\","
            + "\"spark.sql.catalog.manifest_catalog\":\"org.apache.iceberg.spark.SparkCatalog\","
            + "\"spark.sql.catalog.manifest_catalog.type\":\"hadoop\","
            + "\"spark.sql.catalog.manifest_catalog.warehouse\":\""
            + temporaryDirectory.resolve("warehouse")
            + "\"}");
    if (spec != null) {
      jobConf.put("spec_id", spec);
    }
    if (caching != null) {
      jobConf.put("use_caching", caching);
    }
    // Model JobManager's template substitution, including unresolved optional values.
    return new IcebergRewriteManifestsJob()
        .jobTemplate().arguments().stream()
            .map(
                value -> {
                  String resolved = value;
                  for (Map.Entry<String, String> entry : jobConf.entrySet()) {
                    resolved = resolved.replace("{{" + entry.getKey() + "}}", entry.getValue());
                  }
                  return resolved;
                })
            .toArray(String[]::new);
  }

  private void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("TestIcebergRewriteManifestsJob")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.manifest_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.manifest_catalog.type", "hadoop")
            .config(
                "spark.sql.catalog.manifest_catalog.warehouse",
                temporaryDirectory.resolve("warehouse").toString())
            .getOrCreate();
  }

  private void insertRows(int offset) {
    for (int i = 0; i < 3; i++) {
      spark.sql(
          "INSERT INTO "
              + TABLE
              + " VALUES ("
              + (offset + i)
              + ", TIMESTAMP '2026-01-01 01:00:00')");
    }
  }

  private Set<String> manifests(int spec) {
    return spark
        .sql("SELECT path FROM " + TABLE + ".manifests WHERE partition_spec_id = " + spec)
        .collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }

  private List<Row> records() {
    return spark.sql("SELECT * FROM " + TABLE + " ORDER BY id").collectAsList();
  }

  private long currentSnapshot() {
    return spark
        .sql("SELECT snapshot_id FROM " + TABLE + ".refs WHERE name = 'main'")
        .first()
        .getLong(0);
  }
}
