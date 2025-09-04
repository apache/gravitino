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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.logging.log4j.util.Strings;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TrinoQueryIT extends TrinoQueryITBase {
  private static final Logger LOG = LoggerFactory.getLogger(TrinoQueryIT.class);

  protected static String testsetsDir;
  protected AtomicInteger passCount = new AtomicInteger(0);
  protected AtomicInteger totalCount = new AtomicInteger(0);
  protected static boolean exitOnFailed = true;

  // key: tester name, value: tester result
  private static final Map<String, TestStatus> allTestStatus = new TreeMap<>();

  private static final int testParallelism = 2;

  static Map<String, String> queryParams = new HashMap<>();

  static Set<String> ciTestsets = new HashSet<>();

  static TrinoQueryITBase trinoQueryITBase;

  static int trinoWorkerNum = 0;

  static {
    testsetsDir = TrinoQueryIT.class.getClassLoader().getResource("trino-ci-testset").getPath();
    testsetsDir = ITUtils.joinPath(testsetsDir, "testsets");
  }

  @BeforeAll
  public void setup() throws Exception {
    trinoQueryITBase = new TrinoQueryITBase(trinoWorkerNum);
    trinoQueryITBase.setup();
    cleanupTestEnv();

    queryParams.put("mysql_uri", mysqlUri);
    queryParams.put("hive_uri", hiveMetastoreUri);
    queryParams.put("hdfs_uri", hdfsUri);
    queryParams.put("trino_uri", trinoUri);
    queryParams.put("postgresql_uri", postgresqlUri);
    queryParams.put("gravitino_uri", gravitinoUri);

    LOG.info("Test query env parameters: {}", queryParams);
  }

  private static void cleanupTestEnv() throws Exception {
    try {
      Arrays.stream(TrinoQueryITBase.metalake.listCatalogs())
          .filter(catalog -> catalog.startsWith("gt_"))
          .forEach(TrinoQueryITBase::dropCatalog);

      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  String[] catalogs = trinoQueryRunner.runQuery("show catalogs").split("\n");
                  LOG.info("Catalogs: {}", Arrays.toString(catalogs));
                  return Arrays.stream(catalogs).noneMatch(s -> s.startsWith("\"gt_"));
                } catch (Exception e) {
                  LOG.error("Failed to run query in trino", e);
                  return false;
                }
              });
    } catch (Exception e) {
      throw new Exception("Failed to clean up test env: " + e.getMessage(), e);
    }
  }

  @AfterAll
  public static void cleanup() {
    TrinoQueryITBase.cleanup();
  }

  public void runOneTestSetWithCatalog(
      String testSetDirName, String catalog, String testFilterPrefix) throws Exception {
    String[] testerNames = getTesterNames(testSetDirName, testFilterPrefix);
    if (testerNames.length == 0) return;

    String catalogPrefix = "catalog_" + catalog + "_";
    TrinoQueryRunner queryRunner = new TrinoQueryRunner(TrinoQueryITBase.trinoUri);
    executeSqlFile(testSetDirName, catalogPrefix + "prepare.sql", queryRunner, catalog);

    Arrays.sort(testerNames);
    for (String testerName : testerNames) {
      try {
        executeSqlFileWithCheckResult(testSetDirName, testerName, queryRunner, catalog);
      } catch (Exception e) {
        TestStatus testStatus =
            allTestStatus.get(
                TestStatus.testName(ITUtils.joinPath(testSetDirName, testerName), catalog));
        testStatus.endTime = System.currentTimeMillis();
        testStatus.status = false;
        testStatus.error = e.getMessage();

        if (exitOnFailed) {
          throw e;
        }
        LOG.error(e.getMessage());
      }
    }

    executeSqlFile(testSetDirName, catalogPrefix + "cleanup.sql", queryRunner, catalog);
    queryRunner.stop();
  }

  void executeSqlFile(
      String testSetDirName, String filename, TrinoQueryRunner queryRunner, String catalog)
      throws Exception {
    String path = ITUtils.joinPath(testSetDirName, filename);
    String sqls = TrinoQueryITBase.readFileToString(path);
    sqls = removeSqlComments(sqls);

    Matcher sqlMatcher =
        Pattern.compile("([<\\w].*?);", Pattern.DOTALL | Pattern.UNIX_LINES).matcher(sqls);
    while (sqlMatcher.find()) {
      String sql = sqlMatcher.group(1);
      sql = resolveParameters(sql);
      String result = queryRunner.runQuery(sql);
      LOG.info(
          "Execute sql in the tester {} under catalog {} :\n{}\nResult:\n{}",
          simpleTesterName(path),
          catalog,
          sql,
          result);
      if (isQueryFailed(result)) {
        throw new RuntimeException(
            "Failed to execute sql in the test set. "
                + simpleTesterName(path)
                + " under catalog "
                + catalog
                + "Sql: \n"
                + sql
                + "\nResult: \n"
                + result);
      }
    }
  }

  private static String removeSqlComments(String sql) {
    return sql.replaceAll("--.*?\\n", "");
  }

  private static String resolveParameters(String sql) throws Exception {
    Matcher sqlMatcher = Pattern.compile("\\$\\{([\\w_]+)\\}").matcher(sql);
    StringBuffer replacedString = new StringBuffer();
    while (sqlMatcher.find()) {
      String parameter = sqlMatcher.group(1);
      String value = queryParams.get(parameter);
      if (Strings.isEmpty(value)) {
        throw new Exception("Parameter " + parameter + " is not defined in test parameters");
      }
      sqlMatcher.appendReplacement(replacedString, queryParams.get(parameter));
    }
    sqlMatcher.appendTail(replacedString);
    return replacedString.toString();
  }

  private static boolean isQueryFailed(String result) {
    return Pattern.compile("^Query \\w+ failed:").matcher(result).find();
  }

  void executeSqlFileWithCheckResult(
      String testSetDirName, String filename, TrinoQueryRunner queryRunner, String catalog)
      throws Exception {
    String path = ITUtils.joinPath(testSetDirName, filename);
    String sqls = TrinoQueryITBase.readFileToString(path);
    sqls = removeSqlComments(sqls);

    String resultFileName = path.replace(".sql", ".txt");
    String testResults = TrinoQueryITBase.readFileToString(resultFileName);

    Matcher sqlMatcher =
        Pattern.compile("([<\\w].*?);", Pattern.DOTALL | Pattern.UNIX_LINES).matcher(sqls);
    Matcher resultMatcher =
        Pattern.compile("((\".*?\")\\n{2,})|((\\S.*?)\\n{2,})", Pattern.DOTALL | Pattern.UNIX_LINES)
            .matcher(testResults);

    TestStatus testStatus = allTestStatus.get(TestStatus.testName(path, catalog));
    testStatus.startTime = System.currentTimeMillis();
    while (sqlMatcher.find()) {
      String sql = sqlMatcher.group(1);
      sql = resolveParameters(sql);
      String expectResult = "";
      if (resultMatcher.find()) {
        if (resultMatcher.group(2) != null) {
          expectResult = resultMatcher.group(2).trim();
        } else {
          expectResult = resultMatcher.group(4).trim();
        }
      }

      String result = queryRunner.runQuery(sql).trim();
      result = result.replaceAll("WARNING:.*?\\n", "");
      boolean match = match(expectResult, result);

      if (match) {
        LOG.info(
            "Execute sql in the tester {} under catalog {} successfully.\nSql:\n{};\nExpect:\n{}\nActual:\n{}",
            simpleTesterName(path),
            catalog,
            sql,
            expectResult,
            result);
      } else {
        queryRunner.stop();
        String errorMessage =
            String.format(
                "Execute sql in the tester %s under catalog %s failed.\nSql:\n%s;\nExpect:\n%s\nActual:\n%s",
                simpleTesterName(path), catalog, sql, expectResult, result);
        LOG.error(errorMessage);
        throw new Exception(errorMessage);
      }
    }
    testStatus.endTime = System.currentTimeMillis();
    testStatus.status = true;
    testStatus.error = "";
    passCount.incrementAndGet();
    LOG.info("Test progress {}/{}", passCount.get(), totalCount.get());
  }

  /**
   * * This method is used to match the result of the query. There are three cases: 1. The expected
   * result is equal to the actual result. 2. The expected result is a query failed result, and the
   * actual result matches the query failed result. 3. The expected result is a regular expression,
   * and the actual result matches the regular expression.
   *
   * @param expectResult the expected result
   * @param result the actual result
   * @return false if the expected result is empty or the actual result does not match the expected.
   *     For {@literal <BLANK_LINE>} case, return true if the actual result is empty. For {@literal
   *     <QUERY_FAILED>} case, replace the placeholder with "^Query \\w+ failed.*: " and do match.
   */
  static boolean match(String expectResult, String result) {
    if (expectResult.isEmpty()) {
      return false;
    }

    // match black line
    // E.g., the expected result can be matched with the following actual result:
    // query result:
    //
    // expectResult:
    // <BLANK_LINE>
    if (expectResult.equals("<BLANK_LINE>")) {
      return result.isEmpty();
    }

    // Match query failed result.
    // E.g., the expected result can be matched with the following actual result:
    // query result:
    // Query 20240103_132722_00006_pijfx failed: line 8:6: Schema must be specified when session
    // schema is not set
    // expectResult:
    // <QUERY_FAILED> Schema must be specified when session schema is not set
    if (expectResult.startsWith("<QUERY_FAILED>")) {
      return Pattern.compile(
              "^Query \\w+ failed.*: "
                  + Pattern.quote(expectResult.replace("<QUERY_FAILED>", "").trim()))
          .matcher(result)
          .find();
    }

    // match text
    boolean match = expectResult.equals(result);
    if (match) {
      return true;
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
    expectResult = "^\\Q" + expectResult.replace("%", "\\E.*?\\Q") + "\\E$";
    result = result.replace("\n", "");
    match = Pattern.compile(expectResult).matcher(result).find();
    return match;
  }

  @Test
  public void testSql() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(testParallelism);
    CompletionService<Integer> completionService = new ExecutorCompletionService<>(executor);

    String[] testSetNames =
        Arrays.stream(TrinoQueryITBase.listDirectory(testsetsDir))
            .filter(s -> ciTestsets.isEmpty() || ciTestsets.contains(s))
            .toArray(String[]::new);
    List<Future<Integer>> allFutures = new ArrayList<>();
    for (String testSetName : testSetNames) {
      String path = ITUtils.joinPath(testsetsDir, testSetName);
      totalCount.addAndGet(getTesterCount(path, "", ""));
      List<Future<Integer>> futures = runOneTestset(completionService, path, "", "");
      allFutures.addAll(futures);
    }

    waitForCompleted(executor, completionService, allFutures);
  }

  public void testSql(String testSetDirName, String catalog, String testerPrefix) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(testParallelism);
    CompletionService<Integer> completionService = new ExecutorCompletionService<>(executor);

    totalCount.addAndGet(getTesterCount(testSetDirName, catalog, testerPrefix));
    List<Future<Integer>> futures =
        runOneTestset(completionService, testSetDirName, catalog, testerPrefix);
    List<Future<Integer>> allFutures = new ArrayList<>(futures);

    waitForCompleted(executor, completionService, allFutures);
  }

  private void waitForCompleted(
      ExecutorService executor,
      CompletionService<Integer> completionService,
      List<Future<Integer>> allFutures) {
    for (int i = 0; i < allFutures.size(); i++) {
      try {
        Future<Integer> completedTask = completionService.take();
        completedTask.get();
      } catch (InterruptedException | ExecutionException e) {
        executor.shutdownNow();
        throw new RuntimeException("Failed to execute test " + e.getMessage(), e);
      }
    }
    executor.shutdownNow();
    LOG.info(
        "All testers have finished. Total:{}, Pass: {}\n{}",
        totalCount,
        passCount,
        generateTestStatus());
  }

  public String generateTestStatus() {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, TestStatus> entry : allTestStatus.entrySet()) {
      TestStatus testStatus = entry.getValue();
      String status = testStatus.status ? "PASS" : "FAIL";
      builder.append(
          String.format(
              "%s, %s, %.3f, %s\n",
              testStatus.testerName,
              status,
              (testStatus.endTime - testStatus.startTime) / 1000.0,
              testStatus.error));
    }
    return builder.toString();
  }

  public List<Future<Integer>> runOneTestset(
      CompletionService<Integer> completionService,
      String testSetDirName,
      String catalog,
      String testerFilter)
      throws Exception {
    String[] testCatalogs = getTesterCatalogNames(testSetDirName, catalog);

    List<Future<Integer>> futures = new ArrayList<>();
    for (int i = 0; i < testCatalogs.length; i++) {
      int finalI = i;
      futures.add(
          completionService.submit(
              () -> {
                try {
                  runOneTestSetWithCatalog(testSetDirName, testCatalogs[finalI], testerFilter);
                  if (LOG.isDebugEnabled()) {
                    LOG.debug(
                        "Test set {}'s catalog {} run completed",
                        simpleTesterName(testSetDirName),
                        testCatalogs[finalI]);
                  }
                  return finalI;
                } catch (Exception e) {
                  String msg =
                      String.format(
                          "Failed to run the test %s's catalog %s: %s",
                          simpleTesterName(testSetDirName), testCatalogs[finalI], e.getMessage());
                  LOG.error(msg);
                  if (exitOnFailed) {
                    throw new RuntimeException(msg, e);
                  }
                  return finalI;
                }
              }));
    }
    return futures;
  }

  public void runOneTestSetAndGenOutput(
      String testSetDirName, String catalog, String testFilterPrefix) throws Exception {
    String[] testerNames = getTesterNames(testSetDirName, testFilterPrefix);
    String[] catalogNames = getTesterCatalogNames(testSetDirName, catalog);
    if (testerNames.length == 0 || catalogNames.length == 0) {
      return;
    }
    catalog = catalogNames[0];

    String catalogPrefix = catalog.replace("prepare.sql", "");
    TrinoQueryRunner queryRunner = new TrinoQueryRunner(TrinoQueryITBase.trinoUri);
    executeSqlFile(testSetDirName, catalogPrefix + "prepare.sql", queryRunner, catalog);

    Arrays.sort(testerNames);
    for (String testerName : testerNames) {
      executeSqlFileWithGenOutput(testSetDirName, testerName, queryRunner);
    }

    executeSqlFile(testSetDirName, catalogPrefix + "cleanup.sql", queryRunner, catalog);
    queryRunner.stop();
  }

  void executeSqlFileWithGenOutput(
      String testSetDirName, String filename, TrinoQueryRunner queryRunner) throws IOException {
    String path = ITUtils.joinPath(testSetDirName, filename);
    String sqls = TrinoQueryITBase.readFileToString(path);
    String resultFileName = path.replace(".sql", ".txt");
    FileOutputStream outputStream = new FileOutputStream(resultFileName);

    Matcher sqlMatcher =
        Pattern.compile("(\\w.*?);", Pattern.DOTALL | Pattern.UNIX_LINES).matcher(sqls);

    boolean firstLine = true;
    while (sqlMatcher.find()) {
      if (!firstLine) {
        outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
      }
      firstLine = false;

      String sql = sqlMatcher.group(1);
      String result = queryRunner.runQuery(sql).trim();
      LOG.info("Execute sql:\n{}\nResult:\n{}", sql, result);
      if (isQueryFailed(result)) {
        throw new RuntimeException(
            "Failed to execute sql in the test set. "
                + simpleTesterName(path)
                + ":\n"
                + sql
                + "\nresult:\n"
                + result);
      }
      outputStream.write(result.getBytes(StandardCharsets.UTF_8));
      outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    }

    outputStream.close();
  }

  static int getTesterCount(String testSetDirName, String catalog, String testFilterPrefix)
      throws Exception {
    String[] testerNames = getTesterNames(testSetDirName, testFilterPrefix);

    if (Strings.isNotEmpty(catalog)) {
      Arrays.stream(testerNames)
          .forEach(
              name -> {
                TestStatus testStatus =
                    new TestStatus(ITUtils.joinPath(testSetDirName, name), catalog);
                allTestStatus.put(testStatus.testerName, testStatus);
              });
      return testerNames.length;
    }

    String[] testCatalogs = getTesterCatalogNames(testSetDirName, "");
    for (String testCatalog : testCatalogs) {
      Arrays.stream(testerNames)
          .forEach(
              name -> {
                TestStatus testStatus =
                    new TestStatus(ITUtils.joinPath(testSetDirName, name), testCatalog);
                allTestStatus.put(testStatus.testerName, testStatus);
              });
    }

    return testerNames.length * testCatalogs.length;
  }

  static String[] getTesterNames(String testSetDirName, String testFilterPrefix) throws Exception {
    return Arrays.stream(listDirectory(testSetDirName))
        .filter(s -> !s.endsWith("prepare.sql") && !s.endsWith("cleanup.sql") && s.endsWith(".sql"))
        .filter(s -> testFilterPrefix.isEmpty() || s.startsWith(testFilterPrefix))
        .toArray(String[]::new);
  }

  static String[] getTesterCatalogNames(String testSetDirName, String catalog) throws Exception {
    return Arrays.stream(listDirectory(testSetDirName))
        .filter(s -> s.matches("catalog_.*_prepare.sql"))
        .filter(s -> catalog.isEmpty() || s.equals("catalog_" + catalog + "_prepare.sql"))
        .map(s -> s.replace("catalog_", "").replace("_prepare.sql", ""))
        .toArray(String[]::new);
  }

  static String simpleTesterName(String testerName) {
    return testerName.replace(testsetsDir + "/", "");
  }

  static class TestStatus {
    final String testerName;
    String error = "Tester Failed in the preparation stage";
    long startTime = 0;
    long endTime = 0;
    boolean status = false;

    TestStatus(String filename, String catalog) {
      testerName = testName(filename, catalog);
    }

    public static String testName(String filename, String catalog) {
      return catalog + ":" + simpleTesterName(filename);
    }
  }
}
