/*
 * Copyright 2023 Datastrato.
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class TrinoQueryIT {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoQueryIT.class);

  private static boolean isDockerRunning = false;
  private static String gravitinoUri = "http://192.168.215.2:8090";
  private static String trinoUri = "http://192.168.215.2:8080";
  private static String hiveMetastoreUri = "thrift://192.168.215.2:9083";

  private static final String metalakeName = "test";

  private static GravitinoClient client;
  private static GravitinoMetaLake metalake;
  private static String testQueriesDir = "";
  private static AtomicInteger testCount = new AtomicInteger(0);
  private static AtomicInteger totalCount = new AtomicInteger(0);
  static TrinoQueryRunner trinoQueryRunner;
  private static String targetCatalog = null;

  @BeforeAll
  public static void setup() throws Exception {

    try {
      client = GravitinoClient.builder(gravitinoUri).build();
      trinoQueryRunner = new TrinoQueryRunner();

      createMetalake();

      if (targetCatalog == null || targetCatalog.equals("hive")) {
        dropCatalog("hive");
        HashMap<String, String> properties = new HashMap<>();
        properties.put("metastore.uris", hiveMetastoreUri);

        createCatalog("hive", "hive", properties);
      }

      if (targetCatalog == null || targetCatalog.equals("lakehouse-iceberg")) {
        dropCatalog("lakehouse-iceberg");
        HashMap<String, String> properties = new HashMap<>();
        properties.put("uri", hiveMetastoreUri);
        properties.put("catalog-backend", "hive");
        properties.put("warehouse", "hdfs://localhost:9000/user/iceberg/warehouse");
        createCatalog("lakehouse-iceberg", "lakehouse-iceberg", properties);
      }

      isDockerRunning = true;
    } catch (Exception e) {
      LOG.error("Services are not connected", e);
      return;
    }

    testQueriesDir = System.getenv("GRAVITINO_ROOT_DIR");
    if (Strings.isBlank(testQueriesDir)) {
      testQueriesDir = System.getProperty("user.dir");
    }

    if (testQueriesDir.endsWith("integration-test")) {
      testQueriesDir += "/trino-queries/catalogs";
    } else {
      testQueriesDir += "/integration-test/trino-queries/catalogs";
    }

    LOG.info("Test Queries directory is {}", testQueriesDir);

    createMetalake();

    {
      HashMap<String, String> properties = new HashMap<>();
      properties.put("metastore.uris", hiveMetastoreUri);
      createCatalog("hive", "hive", properties);
    }

    {
      HashMap<String, String> properties = new HashMap<>();
      properties.put("uri", hiveMetastoreUri);
      properties.put("catalog-backend", "hive");
      properties.put("warehouse", "hdfs://localhost:9000/user/iceberg/warehouse");
      createCatalog("lakehouse-iceberg", "lakehouse-iceberg", properties);
    }
  }

  @AfterAll
  public static void cleanup() {
    trinoQueryRunner.stop();
    if (targetCatalog == null || targetCatalog.equals("hive")) {
      dropCatalog("hive");
    }
    if (targetCatalog == null || targetCatalog.equals("lakehouse-iceberg")) {
      dropCatalog("lakehouse-iceberg");
    }
  }

  private static void createMetalake() {
    boolean exists = client.metalakeExists(NameIdentifier.of(metalakeName));
    if (exists) {
      metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
      return;
    }

    GravitinoMetaLake createdMetalake =
        client.createMetalake(NameIdentifier.of(metalakeName), "comment", Collections.emptyMap());
    Assertions.assertNotNull(createdMetalake);
    metalake = createdMetalake;
  }

  private static void dropMetalake() {
    boolean exists = client.metalakeExists(NameIdentifier.of(metalakeName));
    if (!exists) {
      return;
    }
    client.dropMetalake(NameIdentifier.of(metalakeName));
  }

  private static void createCatalog(
      String catalogName, String provider, Map<String, String> properties)
      throws InterruptedException {
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
    int tries = 30;
    while (!catalogCreated && tries >= 0) {
      String result = trinoQueryRunner.runQuery("show catalogs");
      if (result.contains(metalakeName + "." + catalogName)) {
        catalogCreated = true;
        break;
      }
      sleep(1000);
      LOG.info("Waiting for catalog {} to be created", catalogName);
    }

    if (!catalogCreated) {
      Assertions.fail("Catalog " + catalogName + " not created");
    }
  }

  private static void dropCatalog(String catalogName) {
    boolean exists = metalake.catalogExists(NameIdentifier.of(metalakeName, catalogName));
    if (!exists) {
      return;
    }
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas schemas = catalog.asSchemas();
    Arrays.stream(schemas.listSchemas(Namespace.ofSchema(metalakeName, catalogName)))
        .filter(schema -> !schema.name().equals("default"))
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
                          } catch (Exception e) {
                            LOG.error("Failed to drop table {}", table);
                          }
                          LOG.info(
                              "Drop table \"{}.{}\".{}.{}",
                              metalakeName,
                              catalogName,
                              schema.name(),
                              table.name());
                        });

                schemas.dropSchema(
                    NameIdentifier.ofSchema(metalakeName, catalogName, schema.name()), true);
              } catch (Exception e) {
                LOG.error("Failed to drop schema {}", schema);
              }
              LOG.info("Drop schema \"{}.{}\".{}", metalakeName, catalogName, schema.name());
            });

    metalake.dropCatalog(NameIdentifier.of(metalakeName, catalogName));
    LOG.info("Drop catalog \"{}.{}\"", metalakeName, catalogName);
  }

  private String[] readCatalogs() throws Exception {
    File dir = new File(testQueriesDir);
    if (dir.exists()) {
      return dir.list();
    }
    throw new Exception("Test queries directory does not exist");
  }

  @Test
  public void testSql() throws Exception {
    if (!isDockerRunning) {
      return;
    }

    String[] catalogNames = readCatalogs();

    ExecutorService executor = Executors.newFixedThreadPool(catalogNames.length);
    Thread.setDefaultUncaughtExceptionHandler(
        (t, e) -> {
          executor.shutdownNow();
        });

    List<Future<?>> futures = new ArrayList<>();
    for (String catalogName : catalogNames) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  runQueriesAndCheck(catalogName, null);
                } catch (Throwable t) {
                  throw new RuntimeException(t);
                }
              }));
    }

    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("Test Failed", e);
        Assertions.fail();
      }
    }
    executor.shutdownNow();
    executor.awaitTermination(3, TimeUnit.SECONDS);
  }

  private String[] loadAllTestFiles(String dirName, String filterPrefix) {
    String targetDir = testQueriesDir + "/" + dirName;
    File targetDirFile = new File(targetDir);
    String[] files = null;
    if (targetDirFile.exists()) {
      if (filterPrefix != null) {
        files = targetDirFile.list((dir, name) -> name.startsWith(filterPrefix));
        totalCount.addAndGet(files.length / 2);
        return files;
      }
      files = targetDirFile.list();
      totalCount.addAndGet(files.length / 2);
      return files;
    }
    return new String[0];
  }

  private String readFileToString(String filename) throws IOException {
    String fileContent = "";
    fileContent = new String(Files.readAllBytes(Paths.get(filename)));
    fileContent += "\n\n";
    return fileContent;
  }

  private void runQueriesAndCheck(String testDirName, String testerId) throws Exception {
    String[] testFileNames = loadAllTestFiles(testDirName, testerId);

    TrinoQueryRunner queryRunner = new TrinoQueryRunner();

    Arrays.sort(testFileNames);
    for (int i = 0; i < testFileNames.length; i += 2) {
      String fileNamePrefix = testFileNames[i].substring(0, testFileNames[i].lastIndexOf('.'));
      fileNamePrefix = testQueriesDir + "/" + testDirName + "/" + fileNamePrefix;
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
          expectResult = resultMatcher.group(1) + "\n";
        }

        String result = queryRunner.runQuery(sql);

        boolean match;

        if (Pattern.compile("^Query \\w+ failed:").matcher(result).find()) {
          match =
              Pattern.compile("^Query \\w+ failed.*: " + expectResult.trim())
                  .matcher(result)
                  .find();
        } else {
          match = expectResult.trim().equals(result.trim());
        }

        if (!match) {
          LOG.error(
              "Test {} failed for query.\nSql:\n{}\nExpect:\n{}\nActual:\n{}",
              testDirName + testFileName.substring(testFileName.lastIndexOf('/')),
              sql,
              expectResult,
              result);
          queryRunner.stop();
          Assertions.fail();
        } else {
          LOG.debug(
              "Test {} success.\nSql:\n{};\nExpect:\n{}\nActual:\n{}",
              testDirName + testFileName.substring(testFileName.lastIndexOf('/')),
              sql,
              expectResult,
              result);
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

  static class TrinoQueryRunner {
    private QueryRunner queryRunner;
    private Terminal terminal;
    private URI uri = new URI("http://192.168.215.2:8080");

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

  public static void main(String[] args) {
    String targetTestId = null;
    try {

      if (args.length == 1) {
        // e.g.: args = hive
        // run hive all the test cases
        targetCatalog = args[0];
        setup();
        TrinoQueryIT testRunner = new TrinoQueryIT();
        testRunner.runQueriesAndCheck(targetCatalog, targetTestId);

      } else if (args.length == 2) {
        // e.g: args = hive 00001
        // run hive test case 00001
        targetCatalog = args[0];
        targetTestId = args[1];

        setup();
        TrinoQueryIT testRunner = new TrinoQueryIT();
        testRunner.runQueriesAndCheck(targetCatalog, targetTestId);
      } else if (args.length == 0) {
        // e.g.: empty args
        // run all catalogs all the test cases
        setup();
        TrinoQueryIT testRunner = new TrinoQueryIT();
        testRunner.testSql();
      }

    } catch (Exception e) {
      LOG.error("", e);
    } finally {
      cleanup();
      System.exit(0);
    }
  }
}
