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
package org.apache.gravitino.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.opentest4j.AssertionFailedError;

public class TestSQLScripts extends TestJDBCBackend {

  @TestTemplate
  public void testSQLScripts() throws SQLException, IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(gravitinoHome, "GRAVITINO_HOME environment variable is not set");
    Path scriptDir = Path.of(gravitinoHome, "scripts", backendType.toLowerCase());

    File[] scriptFiles = scriptDir.toFile().listFiles();
    Assertions.assertNotNull(scriptFiles, "No script files found in " + scriptDir);
    // Sort files to ensure the correct execution order (schema -> upgrade)
    Arrays.sort(scriptFiles, Comparator.comparing(File::getName));

    // A map to store connections for different schema versions
    Pattern schemaPattern =
        Pattern.compile("schema-([\\d.]+)-" + backendType.toLowerCase() + "\\.sql");
    Pattern upgradePattern =
        Pattern.compile("upgrade-([\\d.]+)-to-([\\d.]+)-" + backendType.toLowerCase() + "\\.sql");
    Pattern metricsPattern =
        Pattern.compile(
            "(?:iceberg|optimizer)-metrics-schema-([\\d.]+)-"
                + backendType.toLowerCase()
                + "\\.sql");

    Map<String, List<File>> versionScrips = new HashMap<>();
    for (File scriptFile : scriptFiles) {
      Matcher schemaMatcher = schemaPattern.matcher(scriptFile.getName());
      Matcher upgradeMatcher = upgradePattern.matcher(scriptFile.getName());
      Matcher metricsMatcher = metricsPattern.matcher(scriptFile.getName());

      if (schemaMatcher.matches()) {
        String version = schemaMatcher.group(1);
        versionScrips.computeIfAbsent(version, k -> new ArrayList<>()).add(scriptFile);

      } else if (upgradeMatcher.matches()) {
        String fromVersion = upgradeMatcher.group(1);
        Assertions.assertTrue(
            versionScrips.containsKey(fromVersion), "No schema script found for " + fromVersion);

      } else if (metricsMatcher.matches()) {
        String version = metricsMatcher.group(1);
        versionScrips.computeIfAbsent(version, k -> new ArrayList<>()).add(scriptFile);

      } else {
        Assertions.fail("Unrecognized script file name: " + scriptFile.getName());
      }
    }

    for (List<File> scripts : versionScrips.values()) {
      dropAllTables();
      for (File scriptFile : scripts) {
        executeScript(scriptFile);
      }
    }
  }

  @TestTemplate
  public void testUpgradeSQLScripts() throws SQLException, IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(gravitinoHome, "GRAVITINO_HOME environment variable is not set");
    Path scriptDir = Path.of(gravitinoHome, "scripts", backendType.toLowerCase());
    File[] scriptFiles = scriptDir.toFile().listFiles();
    Assertions.assertNotNull(scriptFiles, "No script files found in " + scriptDir);
    Arrays.sort(scriptFiles, Comparator.comparing(File::getName));

    Pattern upgradePattern =
        Pattern.compile("upgrade-([\\d.]+)-to-([\\d.]+)-" + backendType.toLowerCase() + "\\.sql");
    for (File upgradeScript : scriptFiles) {
      Matcher upgradeMatcher = upgradePattern.matcher(upgradeScript.getName());
      if (!upgradeMatcher.matches()) {
        continue;
      }

      String fromVersion = upgradeMatcher.group(1);
      File sourceSchema =
          scriptDir
              .resolve("schema-" + fromVersion + "-" + backendType.toLowerCase() + ".sql")
              .toFile();
      Assertions.assertTrue(
          sourceSchema.isFile(), "No source schema found for " + upgradeScript.getName());
      dropAllTables();
      executeScript(sourceSchema);
      executeScript(upgradeScript);
    }
  }

  /**
   * The owner unique key allows one live row per (owner, object). Rows left by concurrent
   * assignments are merged during the upgrade: the newest live row (largest id) stays, while older
   * ones are soft-deleted.
   */
  @TestTemplate
  public void testUpgradeToTwoZeroMergesDuplicateLiveOwners() throws SQLException, IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(gravitinoHome, "GRAVITINO_HOME environment variable is not set");
    Path scriptDir = Path.of(gravitinoHome, "scripts", backendType.toLowerCase());
    String suffix = "-" + backendType.toLowerCase() + ".sql";
    dropAllTables();
    executeScript(scriptDir.resolve("schema-1.3.0" + suffix).toFile());

    String insert =
        "INSERT INTO owner_meta (id, metalake_id, owner_id, owner_type, metadata_object_id,"
            + " metadata_object_type, audit_info, current_version, last_version, deleted_at,"
            + " updated_at) VALUES (%d, 1, %d, 'USER', %d, 'CATALOG', '{}', 1, 1, %d, 0)";
    List<String> rows =
        List.of(
            // Three owners of object 10 leave two rows to retire in the same statement.
            String.format(insert, 1, 100, 10, 0),
            String.format(insert, 2, 200, 10, 0),
            String.format(insert, 3, 300, 10, 0),
            // Historical rows may already share a deletion timestamp.
            String.format(insert, 4, 100, 20, 0),
            String.format(insert, 5, 200, 20, 5),
            String.format(insert, 6, 300, 20, 5),
            // Object 10 as a SCHEMA is a different object.
            String.format(insert, 7, 400, 10, 0).replace("'CATALOG'", "'SCHEMA'"));
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      for (String row : rows) {
        statement.execute(row);
      }
    }

    executeScript(scriptDir.resolve("upgrade-1.3.0-to-2.0.0" + suffix).toFile());

    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet live =
            statement.executeQuery("SELECT id FROM owner_meta WHERE deleted_at = 0 ORDER BY id")) {
      List<Long> liveIds = new ArrayList<>();
      while (live.next()) {
        liveIds.add(live.getLong(1));
      }
      Assertions.assertEquals(List.of(3L, 4L, 7L), liveIds);
    }
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet retired =
            statement.executeQuery("SELECT deleted_at, updated_at FROM owner_meta WHERE id = 1")) {
      Assertions.assertTrue(retired.next());
      Assertions.assertTrue(retired.getLong(1) > 0, "older duplicate must be soft-deleted");
      Assertions.assertEquals(retired.getLong(1), retired.getLong(2));
    }
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      // Historical rows can share a deletion timestamp after the upgrade.
      statement.execute(String.format(insert, 8, 500, 20, 5));
      // The existing key still rejects a second live row for the same owner and object.
      Assertions.assertThrows(
          SQLException.class, () -> statement.execute(String.format(insert, 9, 100, 20, 0)));
    }
  }

  /** Verifies the OCC backfill preserves existing history versions on live and deleted rows. */
  @TestTemplate
  public void testUpgradeToTwoZeroBackfillsOccVersions() throws SQLException, IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(gravitinoHome, "GRAVITINO_HOME environment variable is not set");
    Path scriptDir = Path.of(gravitinoHome, "scripts", backendType.toLowerCase());
    String suffix = "-" + backendType.toLowerCase() + ".sql";
    dropAllTables();
    executeScript(scriptDir.resolve("schema-1.3.0" + suffix).toFile());

    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "INSERT INTO fileset_meta (fileset_id, fileset_name, metalake_id, catalog_id, schema_id,"
              + " type, audit_info, current_version, last_version, deleted_at) VALUES"
              + " (1, 'live', 1, 1, 1, 'MANAGED', '{}', 7, 9, 0),"
              + " (2, 'deleted', 1, 1, 1, 'MANAGED', '{}', 5, 5, 100)");
      statement.execute(
          "INSERT INTO policy_meta (policy_id, policy_name, policy_type, metalake_id, audit_info,"
              + " current_version, last_version, deleted_at) VALUES"
              + " (1, 'live', 'custom', 1, '{}', 7, 9, 0),"
              + " (2, 'deleted', 'custom', 1, '{}', 5, 5, 100)");
    }

    executeScript(scriptDir.resolve("upgrade-1.3.0-to-2.0.0" + suffix).toFile());

    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      for (String table : List.of("fileset_meta", "policy_meta")) {
        try (ResultSet rows =
            statement.executeQuery(
                "SELECT current_version, last_version, occ_version, deleted_at FROM "
                    + table
                    + " ORDER BY deleted_at")) {
          Assertions.assertTrue(rows.next());
          Assertions.assertEquals(7, rows.getLong("current_version"));
          Assertions.assertEquals(9, rows.getLong("last_version"));
          Assertions.assertEquals(1, rows.getLong("occ_version"));
          Assertions.assertEquals(0, rows.getLong("deleted_at"));
          Assertions.assertTrue(rows.next());
          Assertions.assertEquals(5, rows.getLong("current_version"));
          Assertions.assertEquals(5, rows.getLong("last_version"));
          Assertions.assertEquals(1, rows.getLong("occ_version"));
          Assertions.assertEquals(100, rows.getLong("deleted_at"));
          Assertions.assertFalse(rows.next());
        }
      }
    }
  }

  /** Verifies a completed upgrade can be repeated without resetting migrated data. */
  @TestTemplate
  public void testUpgradeToTwoZeroIsIdempotent() throws SQLException, IOException {
    Path scriptDir = upgradeScriptDirectory();
    String suffix = "-" + backendType.toLowerCase() + ".sql";
    File upgrade = scriptDir.resolve("upgrade-1.3.0-to-2.0.0" + suffix).toFile();
    dropAllTables();
    executeScript(scriptDir.resolve("schema-1.3.0" + suffix).toFile());
    seedUpgradeData();
    executeScript(upgrade);
    executeStatements(
        List.of(
            "UPDATE idp_user_meta SET enabled = FALSE, audit_info = 'preserved audit'",
            "UPDATE idp_group_meta SET group_comment = 'preserved comment'",
            "UPDATE tag_meta SET allowed_values = '[\"blue\"]'",
            "UPDATE tag_relation_meta SET tag_value = 'blue'",
            "UPDATE fileset_meta SET occ_version = 42",
            "UPDATE policy_meta SET occ_version = 43"),
        "customized migrated data");
    Map<String, Set<String>> expectedColumns = readSchemaColumns();
    List<String> expectedIndexes = readSchemaIndexes(expectedColumns.keySet());
    Map<String, List<List<String>>> expectedData = readUpgradeData();
    executeScript(upgrade);
    executeScript(upgrade);
    Assertions.assertEquals(expectedColumns, readSchemaColumns());
    Assertions.assertEquals(expectedIndexes, readSchemaIndexes(expectedColumns.keySet()));
    Assertions.assertEquals(expectedData, readUpgradeData(), "Retry must preserve migrated values");
    if ("mysql".equals(backendType)) {
      // Neither rename source nor target exists: this is damage, not an already-applied DDL.
      executeStatements(
          List.of("ALTER TABLE schema_meta DROP INDEX schema_meta_idx_mid"),
          "missing target index");
      AssertionFailedError failure =
          Assertions.assertThrows(AssertionFailedError.class, () -> executeScript(upgrade));
      Assertions.assertInstanceOf(SQLException.class, failure.getCause());
      Assertions.assertTrue(
          failure.getCause().getMessage().contains("'idx_mid'"), failure.getMessage());
    }
    if ("h2".equals(backendType)) {
      executeStatements(
          List.of("ALTER INDEX idx_tid_value RENAME TO wrong_idx_tid_value"),
          "rename explicitly named H2 index");
      try {
        Assertions.assertNotEquals(
            expectedIndexes,
            readSchemaIndexes(expectedColumns.keySet()),
            "An explicitly named H2 index must retain its name in schema comparisons");
      } finally {
        executeStatements(
            List.of("ALTER INDEX wrong_idx_tid_value RENAME TO idx_tid_value"),
            "restore explicitly named H2 index");
      }
    }
    assertSchemaMatchesFreshInstall(expectedColumns, expectedIndexes);
  }

  /** Verifies retries after structural DDL and data updates, including prepared DDL execution. */
  @TestTemplate
  public void testUpgradeToTwoZeroRetriesPartialExecution() throws SQLException, IOException {
    Path scriptDir = upgradeScriptDirectory();
    String suffix = "-" + backendType.toLowerCase() + ".sql";
    File schema = scriptDir.resolve("schema-1.3.0" + suffix).toFile();
    File upgrade = scriptDir.resolve("upgrade-1.3.0-to-2.0.0" + suffix).toFile();
    List<String> statements = extractStatements(upgrade.toPath());
    dropAllTables();
    executeScript(schema);
    seedUpgradeData();
    executeScript(upgrade);
    Map<String, Set<String>> expectedColumns = readSchemaColumns();
    List<String> expectedIndexes = readSchemaIndexes(expectedColumns.keySet());
    Map<String, List<List<String>>> expectedData = readUpgradeData();
    assertSchemaMatchesFreshInstall(expectedColumns, expectedIndexes);

    String lastIndexRename =
        statements.stream()
            .filter(sql -> sql.contains("RENAME INDEX"))
            .reduce((first, last) -> last)
            .orElse("");
    boolean testedIndexRename = false;
    for (int completed = 1; completed <= statements.size(); completed++) {
      String last = statements.get(completed - 1).toUpperCase();
      if (!(last.startsWith("ALTER TABLE")
          || last.startsWith("CREATE ")
          || last.startsWith("UPDATE ")
          || last.startsWith("EXECUTE "))) {
        continue;
      }
      // MySQL renames share one guard shape; sample the first and last rename.
      if (last.startsWith("EXECUTE ")
          && completed >= 3
          && statements.get(completed - 3).contains("RENAME INDEX")) {
        if (testedIndexRename && !statements.get(completed - 3).equals(lastIndexRename)) {
          continue;
        }
        testedIndexRename = true;
      }
      dropAllTables();
      executeScript(schema);
      seedUpgradeData();
      // Close the connection after the prefix, as if the migration process stopped here.
      String checkpoint = "Interrupted after statement " + completed + ": " + last;
      executeStatements(statements.subList(0, completed), checkpoint);
      executeScript(upgrade);
      Assertions.assertEquals(expectedColumns, readSchemaColumns(), checkpoint);
      Assertions.assertEquals(
          expectedIndexes, readSchemaIndexes(expectedColumns.keySet()), checkpoint);
      Assertions.assertEquals(expectedData, readUpgradeData(), checkpoint);
      if ("mysql".equals(backendType)) {
        try (SqlSession session =
                SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
            Connection connection = session.getConnection();
            ResultSet keys =
                connection
                    .getMetaData()
                    .getPrimaryKeys(connection.getCatalog(), null, "table_version_info")) {
          List<String> columns = new ArrayList<>();
          while (keys.next()) {
            columns.add(keys.getString("COLUMN_NAME"));
          }
          Assertions.assertEquals(
              Set.of("table_id", "version", "deleted_at"), Set.copyOf(columns), checkpoint);
        }
      }
    }
  }

  /** Verifies unexpected MySQL primary keys are rejected rather than silently accepted. */
  @TestTemplate
  public void testUpgradeToTwoZeroRejectsUnexpectedPrimaryKey() throws SQLException, IOException {
    if (!"mysql".equals(backendType)) {
      return;
    }
    Path scriptDir = upgradeScriptDirectory();
    for (String key :
        List.of("table_id", "version, table_id, deleted_at", "table_id, version, deleted_at")) {
      dropAllTables();
      executeScript(scriptDir.resolve("schema-1.3.0-mysql.sql").toFile());
      seedUpgradeData();
      // The target key is incomplete while the old unique index remains.
      executeStatements(
          List.of("ALTER TABLE table_version_info ADD PRIMARY KEY (" + key + ")"),
          "unexpected primary key");
      boolean keepLegacyIndex = !key.startsWith("version");
      if (!keepLegacyIndex) {
        // Check column order independently of the legacy-index guard.
        executeStatements(
            List.of("ALTER TABLE table_version_info DROP INDEX uk_table_id_version_deleted_at"),
            "remove legacy index");
      }
      AssertionFailedError failure =
          Assertions.assertThrows(
              AssertionFailedError.class,
              () -> executeScript(scriptDir.resolve("upgrade-1.3.0-to-2.0.0-mysql.sql").toFile()));
      Assertions.assertInstanceOf(SQLException.class, failure.getCause());
      String expectedError = keepLegacyIndex ? "primary key" : "uk_table_id_version_deleted_at";
      Assertions.assertTrue(
          failure.getCause().getMessage().toLowerCase().contains(expectedError),
          failure.getMessage());
      Assertions.assertEquals(
          keepLegacyIndex,
          readSchemaIndexes(Set.of("table_version_info")).stream()
              .anyMatch(index -> index.contains("uk_table_id_version_deleted_at")),
          "Failed conversion must preserve the existing indexes");
    }
  }

  private void assertSchemaMatchesFreshInstall(
      Map<String, Set<String>> upgradedColumns, List<String> upgradedIndexes)
      throws SQLException, IOException {
    Map<String, Set<String>> expectedColumns = new TreeMap<>(upgradedColumns);
    // The migration intentionally retains this legacy table; fresh 2.0 installs omit it.
    expectedColumns.keySet().removeIf(table -> table.equalsIgnoreCase("policy_relation_meta"));
    List<String> expectedIndexes =
        upgradedIndexes.stream()
            .filter(index -> !index.toLowerCase().startsWith("policy_relation_meta:"))
            .toList();
    dropAllTables();
    executeScript(
        upgradeScriptDirectory().resolve("schema-2.0.0-" + backendType + ".sql").toFile());
    Map<String, Set<String>> freshColumns = readSchemaColumns();
    Assertions.assertEquals(freshColumns, expectedColumns, "Upgrade must match fresh 2.0 columns");
    Assertions.assertEquals(
        readSchemaIndexes(freshColumns.keySet()),
        expectedIndexes,
        "Upgrade must match fresh 2.0 indexes");
  }

  private Path upgradeScriptDirectory() {
    String home = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(home, "GRAVITINO_HOME environment variable is not set");
    return Path.of(home, "scripts", backendType.toLowerCase());
  }

  private void seedUpgradeData() throws SQLException {
    executeStatements(
        List.of(
            "INSERT INTO idp_user_meta (user_id, user_name, password_hash, "
                + "current_version, last_version) VALUES (1, 'user', 'hash', 7, 9)",
            "INSERT INTO idp_group_meta (group_id, group_name) VALUES (1, 'group')",
            "INSERT INTO tag_meta (tag_id, tag_name, metalake_id, audit_info) VALUES (1, "
                + "'tag', 1, '{}')",
            "INSERT INTO tag_relation_meta (id, tag_id, metadata_object_id, "
                + "metadata_object_type, audit_info) VALUES (1, 1, 1, 'TABLE', '{}')",
            "INSERT INTO fileset_meta (fileset_id, fileset_name, metalake_id, catalog_id, "
                + "schema_id, type, audit_info, current_version, last_version) VALUES (1, "
                + "'fileset', 1, 1, 1, 'MANAGED', '{}', 7, 9)",
            "INSERT INTO policy_meta (policy_id, policy_name, policy_type, metalake_id, "
                + "audit_info, current_version, last_version) VALUES (1, 'policy', 'custom', 1, "
                + "'{}', 7, 9)",
            "INSERT INTO table_version_info (table_id, version, deleted_at) VALUES (1, 7, 0)"),
        "source schema data");
  }

  private Map<String, List<List<String>>> readUpgradeData() throws SQLException {
    Map<String, List<List<String>>> data = new TreeMap<>();
    Map<String, String> selections =
        Map.of(
            "idp_user_meta",
                "user_id, user_name, password_hash, current_version, last_version, enabled, audit_info",
            "idp_group_meta", "group_id, group_name, group_comment, audit_info",
            "tag_meta", "tag_id, tag_name, allowed_values",
            "tag_relation_meta", "id, tag_id, metadata_object_id, tag_value",
            "fileset_meta", "fileset_id, current_version, last_version, occ_version",
            "policy_meta", "policy_id, current_version, last_version, occ_version",
            "table_version_info", "table_id, version, deleted_at");
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      for (Map.Entry<String, String> entry : selections.entrySet()) {
        List<List<String>> rows = new ArrayList<>();
        try (ResultSet result =
            statement.executeQuery(
                "SELECT " + entry.getValue() + " FROM " + entry.getKey() + " ORDER BY 1")) {
          while (result.next()) {
            List<String> row = new ArrayList<>();
            for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
              row.add(result.getString(column));
            }
            rows.add(row);
          }
        }
        data.put(entry.getKey(), rows);
      }
    }
    return data;
  }

  private Map<String, Set<String>> readSchemaColumns() throws SQLException {
    Map<String, Set<String>> columns = new TreeMap<>();
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        ResultSet result =
            connection
                .getMetaData()
                .getColumns(connection.getCatalog(), connection.getSchema(), "%", "%")) {
      while (result.next()) {
        columns
            .computeIfAbsent(result.getString("TABLE_NAME"), key -> new TreeSet<>())
            .add(result.getString("COLUMN_NAME"));
      }
    }
    Assertions.assertFalse(columns.isEmpty(), "Schema metadata must contain columns");
    return columns;
  }

  private List<String> readSchemaIndexes(Set<String> tables) throws SQLException {
    List<String> indexes = new ArrayList<>();
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection()) {
      Set<String> generatedIndexes = new TreeSet<>();
      if ("h2".equals(backendType)) {
        try (PreparedStatement statement =
            connection.prepareStatement(
                "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES "
                    + "WHERE TABLE_SCHEMA = ? AND IS_GENERATED = TRUE")) {
          statement.setString(1, connection.getSchema());
          try (ResultSet result = statement.executeQuery()) {
            while (result.next()) {
              generatedIndexes.add(result.getString("INDEX_NAME"));
            }
          }
        }
      }
      for (String table : tables) {
        try (ResultSet result =
            connection
                .getMetaData()
                .getIndexInfo(
                    connection.getCatalog(), connection.getSchema(), table, false, false)) {
          while (result.next()) {
            if (result.getShort("ORDINAL_POSITION") == 0) {
              continue;
            }
            // H2 appends counters to generated constraint indexes. Keep the constraint name so
            // a named unique index and its fresh-install constraint compare equally; explicit
            // index names are never normalized.
            String name = result.getString("INDEX_NAME");
            if (generatedIndexes.contains(name)) {
              name =
                  name.replaceFirst("_INDEX_[0-9A-F]+$", "")
                      .replaceFirst("^PRIMARY_KEY_[0-9A-F]+$", "");
            }
            indexes.add(
                table
                    + ":"
                    + name
                    + ":"
                    + result.getShort("ORDINAL_POSITION")
                    + ":"
                    + result.getString("COLUMN_NAME")
                    + ":"
                    + result.getBoolean("NON_UNIQUE"));
          }
        }
      }
    }
    indexes.sort(Comparator.naturalOrder());
    return indexes;
  }

  private void executeStatements(List<String> statements, String context) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : statements) {
        Assertions.assertDoesNotThrow(() -> statement.execute(sql), context + ", sql: " + sql);
      }
    }
  }

  private void executeScript(File scriptFile) throws IOException, SQLException {
    List<String> ddls = extractStatements(scriptFile.toPath());
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      for (String ddl : ddls) {
        Assertions.assertDoesNotThrow(
            () -> statement.execute(ddl),
            "Failed to execute DDL in file " + scriptFile.getName() + " ddl: " + ddl);
      }
    }
  }

  private List<String> extractStatements(Path sqlFile) throws IOException {
    String executableSql =
        Files.readAllLines(sqlFile).stream()
            .map(String::trim)
            .filter(line -> !line.isEmpty())
            .filter(line -> !line.startsWith("--"))
            .reduce((left, right) -> left + "\n" + right)
            .orElse("");

    return Arrays.stream(executableSql.split(";"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
  }

  private void dropAllTables() throws SQLException {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        if ("postgresql".equals(backendType)) {
          dropAllTablesForPostgreSQL(connection);
        } else {
          dropAllTablesForMySQLCompatible(connection);
        }
      }
    }
  }

  private void dropAllTablesForMySQLCompatible(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String query = "SHOW TABLES";
      List<String> tableList = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery(query)) {
        while (rs.next()) {
          tableList.add(rs.getString(1));
        }
      }
      for (String table : tableList) {
        statement.execute("DROP TABLE " + table);
      }
    }
  }

  private void dropAllTablesForPostgreSQL(Connection connection) throws SQLException {
    String query =
        "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()";
    List<String> tableList = new ArrayList<>();
    try (ResultSet rs = connection.createStatement().executeQuery(query)) {
      while (rs.next()) {
        tableList.add(rs.getString(1));
      }
    }

    if (tableList.isEmpty()) {
      return;
    }

    for (String table : tableList) {
      connection.createStatement().execute("DROP TABLE " + table);
    }
  }
}
