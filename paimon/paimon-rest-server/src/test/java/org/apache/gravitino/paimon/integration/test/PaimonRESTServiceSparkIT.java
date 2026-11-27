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
package org.apache.gravitino.paimon.integration.test;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.gravitino.paimon.RESTService;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.spark.launcher.JavaModuleOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end test that starts the Paimon REST service and drives it with Spark 3.5 through the
 * Paimon Spark catalog using the REST metastore: database and table creation, writes, schema
 * evolution and tags over real SQL.
 */
public class PaimonRESTServiceSparkIT {

  private static final String CATALOG = "paimon_rest";
  private static final String TABLE = "spark_it_table";
  private static final String TOKEN = "engine-it-token";

  private static RESTService service;
  private static int port;
  @TempDir static Path warehouse;

  @BeforeAll
  static void start() throws Exception {
    port = findRandomPort();
    service = new RESTService();
    Map<String, String> config =
        ImmutableMap.of(
            "host",
            "127.0.0.1",
            "httpPort",
            String.valueOf(port),
            "catalog-backend",
            "filesystem",
            "warehouse",
            warehouse.toUri().toString(),
            "token.spark-it",
            TOKEN);
    service.serviceInit(config, false);
    service.serviceStart();
  }

  @AfterAll
  static void stop() throws Exception {
    if (service != null) {
      service.serviceStop();
    }
  }

  @Test
  void testSparkSqlOverRestCatalog(@TempDir Path directory) throws Exception {
    String version = System.getProperty("paimon.test.engine.version");
    runEngineClient(version, "spark-ddl", "spark_it_db", false, directory);
  }

  private static void verifySparkSqlOverRestCatalog(SparkSession spark, String database) {
    spark.sql("CREATE DATABASE " + CATALOG + "." + database);

    spark.sql(
        String.format("CREATE TABLE %s.%s.%s (id INT, name STRING)", CATALOG, database, TABLE));

    spark.sql(
        String.format(
            "INSERT INTO %s.%s.%s VALUES (1, 'alice'), (2, 'bob')", CATALOG, database, TABLE));
    List<org.apache.spark.sql.Row> rows =
        spark
            .sql(
                String.format(
                    "SELECT id, name FROM %s.%s.%s ORDER BY id", CATALOG, database, TABLE))
            .collectAsList();
    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals("alice", rows.get(0).getString(1));

    String replacement = CATALOG + "." + database + ".replacement";
    spark.sql("CREATE TABLE " + replacement + " (id INT)");
    spark.sql("INSERT INTO " + replacement + " VALUES (1)");
    spark.sql("CREATE OR REPLACE TABLE " + replacement + " (id INT, name STRING)");
    Assertions.assertEquals(0L, spark.table(replacement).count());
    Assertions.assertEquals(2, spark.table(replacement).schema().fields().length);
    spark.sql("REPLACE TABLE " + replacement + " (id INT)");
    Assertions.assertEquals(1, spark.table(replacement).schema().fields().length);
    spark.sql("DROP TABLE " + replacement);

    // Schema evolution through the REST server.
    spark.sql(String.format("ALTER TABLE %s.%s.%s ADD COLUMN age INT", CATALOG, database, TABLE));
    spark.sql(
        String.format("INSERT INTO %s.%s.%s VALUES (3, 'carol', 30)", CATALOG, database, TABLE));
    List<org.apache.spark.sql.Row> rowsAfterEvolution =
        spark
            .sql(String.format("SELECT COUNT(*) FROM %s.%s.%s", CATALOG, database, TABLE))
            .collectAsList();
    Assertions.assertEquals(3, rowsAfterEvolution.get(0).getLong(0));

    // Tags through the REST server.
    spark.sql(String.format("ALTER TABLE %s.%s.%s CREATE TAG `it-tag`", CATALOG, database, TABLE));
    Dataset<org.apache.spark.sql.Row> tagHistory =
        spark.sql(
            String.format(
                "SELECT * FROM %s.%s.%s VERSION AS OF 'it-tag'", CATALOG, database, TABLE));
    Assertions.assertTrue(tagHistory.count() >= 2);

    // Rollback through the REST server.
    spark.sql(
        String.format(
            "CALL %s.sys.rollback(table => '%s.%s', version => '1')", CATALOG, database, TABLE));
    List<org.apache.spark.sql.Row> rowsAfterRollback =
        spark
            .sql(String.format("SELECT COUNT(*) FROM %s.%s.%s", CATALOG, database, TABLE))
            .collectAsList();
    // Snapshot 1 holds the first insert (2 rows).
    Assertions.assertEquals(2, rowsAfterRollback.get(0).getLong(0));
  }

  @Test
  void testFlinkAndSparkWithUpgradedZstd(@TempDir Path directory) throws Exception {
    verifyFlinkAndSpark(System.getProperty("paimon.test.engine.version"), true, directory);
  }

  @ParameterizedTest
  @ValueSource(strings = {"1.4.2", "2.0.0", "2.1.0"})
  void testFlinkAndSparkWithPaimonClientVersion(String version, @TempDir Path directory)
      throws Exception {
    verifyFlinkAndSpark(version, false, directory);
  }

  private static void verifyFlinkAndSpark(String version, boolean patched, Path directory)
      throws Exception {
    String database = "flink_it_db_" + (patched ? "patched_zstd" : version.replace('.', '_'));
    for (String scenario : List.of("flink-write", "spark-append", "flink-read")) {
      runEngineClient(version, scenario, database, patched, directory);
    }
  }

  private static void runEngineClient(
      String version, String scenario, String database, boolean patched, Path directory)
      throws Exception {
    String engine = scenario.startsWith("flink-") ? "flink" : "spark";
    Path classpath =
        Path.of(
            System.getProperty("paimon.test.engine.classpath-directory"),
            (patched ? "patched" : version) + "-" + engine);
    Path log = directory.resolve(scenario + ".log");
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(Arrays.asList(JavaModuleOptions.defaultModuleOptions().split("\\s+")));
    if (patched) {
      command.add("-Dpaimon.zstd-jni.version=" + System.getProperty("paimon.zstd-jni.version"));
    }
    command.addAll(
        List.of(
            "-cp",
            Files.readString(classpath),
            PaimonRESTServiceSparkIT.class.getName(),
            restUri(),
            version,
            scenario,
            database));
    Process client =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      Assertions.assertTrue(
          client.waitFor(150, TimeUnit.SECONDS),
          "Timed out running " + scenario + " with Paimon " + version);
      Assertions.assertEquals(0, client.exitValue(), () -> readClientLog(log));
    } finally {
      if (client.isAlive()) {
        client.destroyForcibly();
        client.waitFor(5, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Runs the cross-engine test with only one engine connector on the client classpath.
   *
   * @param args REST catalog URI, expected connector version, test scenario, and database name
   * @throws Exception if either engine fails to commit or read data
   */
  public static void main(String[] args) throws Exception {
    String uri = args[0];
    String version = args[1];
    String scenario = args[2];
    String database = args[3];
    Class<?> connector =
        scenario.startsWith("flink-") ? FlinkCatalogFactory.class : SparkCatalog.class;
    URL artifact = connector.getProtectionDomain().getCodeSource().getLocation();
    Assertions.assertTrue(artifact.getPath().endsWith("-" + version + ".jar"), artifact::toString);
    Assertions.assertEquals(
        artifact, Catalog.class.getProtectionDomain().getCodeSource().getLocation());
    String zstdVersion = System.getProperty("paimon.zstd-jni.version");
    if (zstdVersion != null) {
      verifyPatchedZstd(connector.getClassLoader(), zstdVersion);
    }
    switch (scenario) {
      case "flink-write":
        writeFlinkCheckpoint(uri, database);
        break;
      case "spark-ddl":
        try (SparkSession spark = newSparkSession(uri)) {
          verifySparkSqlOverRestCatalog(spark, database);
        }
        break;
      case "spark-append":
        try (SparkSession spark = newSparkSession(uri)) {
          verifyAndAppendWithSpark(spark, database);
        }
        break;
      case "flink-read":
        verifyFlinkRead(uri, database);
        break;
      default:
        throw new IllegalArgumentException("Unknown test scenario " + scenario);
    }
  }

  private static void verifyPatchedZstd(ClassLoader loader, String version) throws Exception {
    Assertions.assertEquals(
        1, Collections.list(loader.getResources("com/github/luben/zstd/Zstd.class")).size());
    Class<?> zstd = loader.loadClass("com.github.luben.zstd.Zstd");
    String zstdSource = zstd.getProtectionDomain().getCodeSource().getLocation().getPath();
    Assertions.assertTrue(zstdSource.endsWith("/zstd-jni-" + version + ".jar"), zstdSource);
  }

  @SuppressWarnings("deprecation") // The test source emits both records under the checkpoint lock.
  private static void writeFlinkCheckpoint(String uri, String database) throws Exception {
    String table = CATALOG + "." + database + ".events";
    Configuration flinkConfig = new Configuration();
    flinkConfig.set(RestOptions.BIND_PORT, "0");
    StreamExecutionEnvironment environment =
        StreamExecutionEnvironment.createLocalEnvironment(1, flinkConfig);
    environment.enableCheckpointing(250);
    StreamTableEnvironment flink = StreamTableEnvironment.create(environment);
    registerFlinkCatalog(flink, uri);
    flink.executeSql("CREATE DATABASE " + CATALOG + "." + database);
    flink.executeSql(
        "CREATE TABLE "
            + table
            + " (id INT, name STRING) WITH ('file.format' = 'parquet', 'file.compression' = 'zstd')");
    flink.createTemporaryView(
        "input_events",
        environment.addSource(
            new TwoRowSource(),
            Types.ROW_NAMED(new String[] {"id", "name"}, Types.INT, Types.STRING)));

    JobClient writeJob =
        flink
            .executeSql("INSERT INTO " + table + " SELECT * FROM input_events")
            .getJobClient()
            .orElseThrow();
    try (Catalog client =
        CatalogFactory.createCatalog(
            CatalogContext.create(
                Options.fromMap(
                    Map.of(
                        "metastore",
                        "rest",
                        "uri",
                        uri,
                        "warehouse",
                        "default_catalog",
                        "token.provider",
                        "bear",
                        "token",
                        TOKEN))))) {
      Identifier id = Identifier.create(database, "events");
      long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
      while (client.loadSnapshot(id).isEmpty()) {
        if (writeJob.getJobStatus().get(5, TimeUnit.SECONDS).isTerminalState()) {
          writeJob.getJobExecutionResult().get(5, TimeUnit.SECONDS);
          Assertions.fail("Flink job finished before committing a checkpoint");
        }
        Assertions.assertTrue(System.nanoTime() < deadline, "Flink checkpoint did not commit");
        Thread.sleep(200);
      }
    } finally {
      writeJob.cancel().get(30, TimeUnit.SECONDS);
    }
  }

  private static void verifyAndAppendWithSpark(SparkSession spark, String database) {
    String table = CATALOG + "." + database + ".events";
    Assertions.assertEquals(
        List.of(1, 2),
        spark.sql("SELECT id FROM " + table + " ORDER BY id").collectAsList().stream()
            .map(row -> row.getInt(0))
            .collect(Collectors.toList()));
    spark.sql("INSERT INTO " + table + " VALUES (3, 'carol'), (4, 'dan')");
  }

  private static void verifyFlinkRead(String uri, String database) throws Exception {
    String table = CATALOG + "." + database + ".events";
    TableEnvironment batch = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    registerFlinkCatalog(batch, uri);
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> results =
        batch.executeSql("SELECT id, name FROM " + table + " ORDER BY id").collect()) {
      results.forEachRemaining(rows::add);
    }
    Assertions.assertEquals(
        List.of(Row.of(1, "alice"), Row.of(2, "bob"), Row.of(3, "carol"), Row.of(4, "dan")), rows);
  }

  private static SparkSession newSparkSession(String uri) {
    return SparkSession.builder()
        .master("local[1]")
        .appName("paimon-rest-spark-it")
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
        .config("spark.sql.catalog." + CATALOG, "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog." + CATALOG + ".metastore", "rest")
        .config("spark.sql.catalog." + CATALOG + ".uri", uri)
        .config("spark.sql.catalog." + CATALOG + ".warehouse", "default_catalog")
        .config("spark.sql.catalog." + CATALOG + ".token.provider", "bear")
        .config("spark.sql.catalog." + CATALOG + ".token", TOKEN)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate();
  }

  private static String readClientLog(Path log) {
    try {
      return Files.readString(log);
    } catch (IOException e) {
      return "Could not read the engine client log: " + e.getMessage();
    }
  }

  private static void registerFlinkCatalog(TableEnvironment flink, String uri) {
    flink.executeSql(
        "CREATE CATALOG "
            + CATALOG
            + " WITH ("
            + "'type'='paimon', 'metastore'='rest', 'uri'='"
            + uri
            + "',"
            + "'warehouse'='default_catalog', 'token.provider'='bear', 'token'='"
            + TOKEN
            + "')");
  }

  private static String restUri() {
    return "http://127.0.0.1:" + port + "/paimon/";
  }

  private static int findRandomPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @SuppressWarnings("deprecation")
  private static class TwoRowSource implements SourceFunction<Row> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Row> context) throws InterruptedException {
      synchronized (context.getCheckpointLock()) {
        context.collect(Row.of(1, "alice"));
        context.collect(Row.of(2, "bob"));
      }
      // Keep the source alive so that visibility requires an actual streaming checkpoint.
      while (running) {
        Thread.sleep(50);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }
}
