/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import static io.trino.cli.ClientOptions.OutputFormat.CSV;
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
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.TableCatalog;
import io.trino.cli.Query;
import io.trino.cli.QueryRunner;
import io.trino.cli.TerminalUtils;
import io.trino.client.ClientSession;
import io.trino.client.uri.TrinoUri;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jodd.io.StringOutputStream;
import okhttp3.logging.HttpLoggingInterceptor;
import org.apache.logging.log4j.util.Strings;
import org.jline.terminal.Terminal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class TrinoQueryIT {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoQueryIT.class);

  private static boolean autoStartEnv = true;

  // TODO(yuhui) redo get the configs after we have the Docker image ready for testing.
  private static String gravitinoUri = "http://127.0.0.1:8090";
  private static String trinoUri = "http://127.0.0.1:8080";
  private static String hiveMetastoreUri = "thrift://127.0.0.1:9083";
  private static String hdfsUri = "hdfs://127.0.0.1:9000";
  private static String mysqlUri = "jdbc:mysql://127.0.0.1";
  private static String postgresqlUri = "jdbc:postgresql://127.0.0.1";

  private static GravitinoClient gravitinoClient;
  private static TrinoITContainers trinoITContainers;
  private static TrinoQueryRunner trinoQueryRunner;

  private static final String metalakeName = "test";
  private static GravitinoMetaLake metalake;
  private static final Set<String> testCatalogs = new HashSet<>();

  private static String testQueriesDir = "";
  private static AtomicInteger testCount = new AtomicInteger(0);
  private static AtomicInteger totalCount = new AtomicInteger(0);

  private static void setEnv() throws Exception {
    if (autoStartEnv) {
      AbstractIT.startIntegrationTest();
      trinoITContainers = ContainerSuite.getInstance().getTrinoITContainers();
      trinoITContainers.launch(AbstractIT.getGravitinoServerPort());
      gravitinoClient = AbstractIT.getGravitinoClient();

      gravitinoUri = String.format("http://127.0.0.1:%d", AbstractIT.getGravitinoServerPort());
      trinoUri = trinoITContainers.getTrinoUri();
      hiveMetastoreUri = trinoITContainers.getHiveMetastoreUri();
      hdfsUri = trinoITContainers.getHdfsUri();
      mysqlUri = trinoITContainers.getMysqlUri();
      postgresqlUri = trinoITContainers.getPostgresqlUri();

    } else {
      gravitinoClient = GravitinoClient.builder(gravitinoUri).build();
    }
  }

  @BeforeAll
  public static void setup() throws Exception {
    setEnv();

    trinoQueryRunner = new TrinoQueryRunner();

    createMetalake();

    Set<String> catalogs = new HashSet<>(testCatalogs);

    if (catalogs.isEmpty() || catalogs.contains("hive")) {
      dropCatalog("hive");
      HashMap<String, String> properties = new HashMap<>();
      properties.put("metastore.uris", hiveMetastoreUri);

      createCatalog("hive", "hive", properties);
    }

    if (catalogs.isEmpty() || catalogs.contains("lakehouse-iceberg")) {
      dropCatalog("lakehouse-iceberg");
      HashMap<String, String> properties = new HashMap<>();
      properties.put("uri", hiveMetastoreUri);
      properties.put("catalog-backend", "hive");
      properties.put("warehouse", hdfsUri + "/user/iceberg/warehouse/TrinoQueryIT");

      createCatalog("lakehouse-iceberg", "lakehouse-iceberg", properties);
    }

    if (catalogs.isEmpty() || catalogs.contains("jdbc-mysql")) {
      dropCatalog("jdbc-mysql");
      HashMap<String, String> properties = new HashMap<>();
      properties.put("jdbc-url", mysqlUri + "?useSSL=false");
      properties.put("jdbc-user", "trino");
      properties.put("jdbc-password", "ds123");
      properties.put("jdbc-driver", "com.mysql.cj.jdbc.Driver");

      createCatalog("jdbc-mysql", "jdbc-mysql", properties);
    }

    if (catalogs.isEmpty() || catalogs.contains("jdbc-postgresql")) {
      dropCatalog("jdbc-postgresql");
      HashMap<String, String> properties = new HashMap<>();
      properties.put("jdbc-url", postgresqlUri + "/gt_db");
      properties.put("jdbc-user", "trino");
      properties.put("jdbc-password", "ds123");
      properties.put("jdbc-database", "gt_db");
      properties.put("jdbc-driver", "org.postgresql.Driver");

      createCatalog("jdbc-postgresql", "jdbc-postgresql", properties);
    }

    testQueriesDir =
        TrinoQueryIT.class.getClassLoader().getResource("trino-queries/catalogs").getPath();
    LOG.info("Test Queries directory is {}", testQueriesDir);
  }

  @AfterAll
  public static void cleanup() {
    if (trinoQueryRunner != null) {
      trinoQueryRunner.stop();
    }

    try {
      if (autoStartEnv) {
        trinoITContainers.shutdown();
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
      testCatalogs.add(catalogName);
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

  private static void dropCatalog(String catalogName) {
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
                          try {
                            tableCatalog.purgeTable(
                                NameIdentifier.ofTable(
                                    metalakeName, catalogName, schema.name(), table.name()));
                          } catch (UnsupportedOperationException e) {
                            tableCatalog.dropTable(
                                NameIdentifier.ofTable(
                                    metalakeName, catalogName, schema.name(), table.name()));
                            LOG.info(
                                "Drop table \"{}.{}\".{}.{}",
                                metalakeName,
                                catalogName,
                                schema.name(),
                                table.name());
                          } catch (Exception e) {
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

  private String[] readCatalogNames() throws Exception {
    File dir = new File(testQueriesDir);
    if (dir.exists()) {
      return dir.list();
    }
    throw new Exception("Test queries directory does not exist");
  }

  @Test
  public void testSql() throws Exception {
    String[] catalogNames = readCatalogNames();

    ExecutorService executor = Executors.newFixedThreadPool(catalogNames.length);
    CompletionService completionService = new ExecutorCompletionService<>(executor);

    List<Future<Integer>> futures = new ArrayList<>();
    for (int i = 0; i < catalogNames.length; i++) {
      int finalI = i;
      futures.add(
          completionService.submit(
              () -> {
                try {
                  runQueriesAndCheck(catalogNames[finalI], null);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
                LOG.debug("Catalog {} test run completed", catalogNames[finalI]);
                return finalI;
              }));
    }
    for (int i = 0; i < catalogNames.length; i++) {
      try {
        Future<Integer> completedTask = completionService.take();
        Integer taskId = completedTask.get();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Test failed: ", e);
        Assertions.fail("Test failed: " + e.getMessage());
      }
    }

    testCatalogs.stream().forEach(catalog -> dropCatalog(catalog));
    Log.info("All tests completed");
    executor.shutdownNow();
    executor.awaitTermination(3, TimeUnit.SECONDS);
  }

  private String[] loadAllTestFiles(String dirName, String filterPrefix) {
    String targetDir = ITUtils.joinPath(testQueriesDir, dirName);
    File targetDirFile = new File(targetDir);
    if (!targetDirFile.exists()) {
      return new String[0];
    }

    String[] files =
        Arrays.stream(targetDirFile.list())
            .filter(name -> filterPrefix == null || name.startsWith(filterPrefix))
            .toArray(String[]::new);

    totalCount.addAndGet(files.length / 2);
    return files;
  }

  private String readFileToString(String filename) throws IOException {
    return new String(Files.readAllBytes(Paths.get(filename))) + "\n\n";
  }

  private void runQueriesAndCheck(String testDirName, String testerId) throws Exception {
    String[] testFileNames = loadAllTestFiles(testDirName, testerId);

    TrinoQueryRunner queryRunner = new TrinoQueryRunner();

    Arrays.sort(testFileNames);
    for (int i = 0; i < testFileNames.length; i += 2) {
      String fileNamePrefix = testFileNames[i].substring(0, testFileNames[i].lastIndexOf('.'));
      fileNamePrefix = ITUtils.joinPath(testQueriesDir, testDirName, fileNamePrefix);
      String testFileName = fileNamePrefix + ".sql";
      String testResultFileName = fileNamePrefix + ".txt";

      String testSql = readFileToString(testFileName);
      String testResult = readFileToString(testResultFileName);

      Matcher sqlMatcher =
          Pattern.compile("(\\w.*?);", Pattern.DOTALL | Pattern.UNIX_LINES).matcher(testSql);
      Matcher resultMatcher =
          Pattern.compile("(\\S.*?)\\n{2,}", Pattern.DOTALL | Pattern.UNIX_LINES)
              .matcher(testResult);

      while (sqlMatcher.find()) {
        String sql = sqlMatcher.group(1);
        String expectResult = "";
        if (resultMatcher.find()) {
          expectResult = resultMatcher.group(1).trim();
        }

        String result = queryRunner.runQuery(sql).trim();
        boolean match = match(expectResult, result);

        if (match) {
          LOG.info(
              "Test {} success.\nSql:\n{};\nExpect:\n{}\nActual:\n{}",
              testDirName + testFileName.substring(testFileName.lastIndexOf('/')),
              sql,
              expectResult,
              result);
        } else {
          queryRunner.stop();
          String errorMessage =
              String.format(
                  "Test %s failed for query.\nSql:\n%s\nExpect:\n%s\nActual:\n%s",
                  testDirName + testFileName.substring(testFileName.lastIndexOf('/')),
                  sql,
                  expectResult,
                  result);
          LOG.error(errorMessage);
          Assertions.fail(errorMessage);
        }
      }
      testCount.incrementAndGet();
      LOG.info(
          "Test {} success. progress {}/{}",
          testDirName + testFileName.substring(testFileName.lastIndexOf('/')),
          testCount.get(),
          totalCount.get());
    }
    queryRunner.stop();
  }

  /**
   * * This method is used to match the result of the query. There are three cases: 1. The expected
   * result is equal to the actual result. 2. The expected result is a query failed result, and the
   * actual result matches the query failed result. 3. The expected result is a regular expression,
   * and the actual result matches the regular expression.
   *
   * @param expectResult
   * @param result
   * @return
   */
  boolean match(String expectResult, String result) {
    if (expectResult.isEmpty()) {
      return false;
    }

    boolean match = expectResult.equals(result);
    if (match) {
      return true;
    }

    // Match query failed result.
    // E.g., the expected result can be matched with the following actual result:
    // query result:
    // Query 20240103_132722_00006_pijfx failed: line 8:6: Schema must be specified when session
    // schema is not set
    // expectResult:
    // Schema must be specified when session schema is not set
    if (Pattern.compile("^Query \\w+ failed:").matcher(result).find()) {
      match = Pattern.compile("^Query \\w+ failed.*: " + expectResult).matcher(result).find();
      return match;
    }

    // Match Wildcard.
    // The valid wildcard is '%'. It can match any character.
    // E.g., the expected result can be matched with the following actual result:
    // query result:
    //    ...
    //    location = 'hdfs://10.1.30.1:9000/user/hive/warehouse/gt_db1.db/tb01',
    //    ...
    // expectResult:
    //
    //    location = 'hdfs://%:9000/user/hive/warehouse/gt_db1.db/tb01',
    //    ...
    expectResult = expectResult.replace("\n", "");
    expectResult = "\\Q" + expectResult.replace("%", "\\E.*?\\Q") + "\\E";
    result = result.replace("\n", "");
    match = Pattern.compile(expectResult).matcher(result).find();
    return match;
  }

  static class TrinoQueryRunner {
    private QueryRunner queryRunner;
    private Terminal terminal;
    private URI uri;

    TrinoQueryRunner() throws Exception {
      this.uri = new URI(trinoUri);
      this.queryRunner = createQueryRunner();
      this.terminal = TerminalUtils.getTerminal();
    }

    private QueryRunner createQueryRunner() throws Exception {

      TrinoUri trinoUri = TrinoUri.builder().setUri(uri).build();

      ClientSession session =
          ClientSession.builder()
              .server(uri)
              .user(Optional.of("admin"))
              .timeZone(ZoneId.systemDefault())
              .build();
      return new QueryRunner(trinoUri, session, false, HttpLoggingInterceptor.Level.NONE);
    }

    String runQuery(String query) {
      Query queryResult = queryRunner.startQuery(query);
      StringOutputStream outputStream = new StringOutputStream();
      queryResult.renderOutput(
          this.terminal,
          new PrintStream(outputStream),
          new PrintStream(outputStream),
          CSV,
          Optional.of(""),
          false);
      ClientSession session = queryRunner.getSession();

      // update catalog and schema if present
      if (queryResult.getSetCatalog().isPresent() || queryResult.getSetSchema().isPresent()) {
        ClientSession.Builder builder = ClientSession.builder(session);
        queryResult.getSetCatalog().ifPresent(builder::catalog);
        queryResult.getSetSchema().ifPresent(builder::schema);
        session = builder.build();
        queryRunner.setSession(session);
      }
      return outputStream.toString();
    }

    boolean stop() {
      try {
        queryRunner.close();
        terminal.close();
        return true;
      } catch (Exception e) {
        LOG.error("Failed to stop query runner", e);
        return false;
      }
    }
  }

  private static void checkEnv() {
    if (Strings.isEmpty(System.getenv("GRAVITINO_ROOT_DIR"))) {
      throw new RuntimeException("GRAVITINO_ROOT_DIR is not set");
    }

    if (Strings.isEmpty(System.getenv("GRAVITINO_HOME"))) {
      throw new RuntimeException("GRAVITINO_HOME is not set");
    }

    if (Strings.isEmpty(System.getenv("GRAVITINO_TEST"))) {
      throw new RuntimeException("GRAVITINO_TEST is not set");
    }
  }

  public static void main(String[] args) {
    checkEnv();
    String targetTestId = null;
    try {
      if (args.length == 1) {
        // e.g.: args = hive
        // run hive all the test cases
        String catalog = args[0];
        testCatalogs.add(catalog);
        setup();
        TrinoQueryIT testRunner = new TrinoQueryIT();
        testRunner.runQueriesAndCheck(catalog, targetTestId);

      } else if (args.length == 2) {
        // e.g: args = hive 00001
        // run hive test case 00001
        String catalog = args[0];
        targetTestId = args[1];

        testCatalogs.add(catalog);
        setup();
        TrinoQueryIT testRunner = new TrinoQueryIT();
        testRunner.runQueriesAndCheck(catalog, targetTestId);

      } else if (args.length == 0) {
        // e.g.: empty args
        // run all catalogs all the test cases
        setup();
        TrinoQueryIT testRunner = new TrinoQueryIT();
        testRunner.testSql();
        return;
      } else {
        throw new IllegalArgumentException(
            "Invalid args: " + Arrays.toString(args) + "\n" + "Usage: ./run [catalog] [testId]");
      }
      Log.info("All tests completed");
      testCatalogs.stream().forEach(c -> dropCatalog(c));
    } catch (Exception e) {
      LOG.error("", e);
    } finally {
      cleanup();
      System.exit(0);
    }
  }
}
