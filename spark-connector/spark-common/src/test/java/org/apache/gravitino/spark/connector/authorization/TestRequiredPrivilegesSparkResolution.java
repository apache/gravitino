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

package org.apache.gravitino.spark.connector.authorization;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Drives a real Spark analyzer to prove how {@link AuthorizationTable} and {@link
 * RequiredPrivilegesCheck} behave end to end: every relation is resolved first (Spark calls {@code
 * schema()} on each denied table while building the relation), and the post-hoc resolution rule
 * then reports all denied tables together, before analysis fails for any other reason.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRequiredPrivilegesSparkResolution {

  private SparkSession spark;

  @BeforeAll
  void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("test-authorization-resolution")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.denied", DeniedTableCatalog.class.getName())
            .config(
                StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(),
                GravitinoAuthorizationSparkSessionExtensions.class.getName())
            .getOrCreate();
  }

  @AfterAll
  void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
    AuthorizationTable.clear();
  }

  @AfterEach
  void clearCollector() {
    AuthorizationTable.clear();
  }

  @Test
  void testSelectAcrossDeniedTablesReportsEveryTable() {
    ForbiddenException failure =
        assertThrows(
            ForbiddenException.class,
            () -> spark.sql("SELECT 1 FROM denied.db.t1 UNION ALL SELECT 1 FROM denied.db.t2"));

    // Both relations were resolved before the failure was raised, so a single error lists them all.
    assertTrue(failure.getMessage().contains("denied.db.t1: SELECT_TABLE"), failure.getMessage());
    assertTrue(failure.getMessage().contains("denied.db.t2: SELECT_TABLE"), failure.getMessage());
  }

  @Test
  void testInsertIntoDeniedTableReportsForbidden() {
    ForbiddenException failure =
        assertThrows(
            ForbiddenException.class,
            () -> spark.sql("INSERT INTO denied.db.t1 SELECT * FROM denied.db.t2"));

    // Both the write target and the read source are denied and reported together.
    assertTrue(failure.getMessage().contains("denied.db.t1: SELECT_TABLE"), failure.getMessage());
    assertTrue(failure.getMessage().contains("denied.db.t2: SELECT_TABLE"), failure.getMessage());
  }

  @Test
  void testColumnReferenceReportsForbiddenInsteadOfUnresolvedColumn() {
    // The denied table exposes an empty placeholder schema, so "missing_col" cannot be resolved.
    // The post-hoc rule runs before checkAnalysis, so the authorization failure wins over the
    // "cannot resolve column" error.
    ForbiddenException failure =
        assertThrows(
            ForbiddenException.class, () -> spark.sql("SELECT missing_col FROM denied.db.t1"));

    assertTrue(failure.getMessage().contains("denied.db.t1: SELECT_TABLE"), failure.getMessage());
  }

  @Test
  void testSqlParserClearsStaleDeniedTablesBeforeNextQuery() throws Exception {
    AuthorizationTable.deny(
        "stale_table",
        "catalog.schema.stale_table",
        ImmutableSet.of(Privilege.Name.SELECT_TABLE),
        new ForbiddenException("denied stale table"));

    spark.sessionState().sqlParser().parsePlan("SELECT 1");

    assertFalse(AuthorizationTable.drainFailure().isPresent());
  }

  /** A catalog that denies every table, used to exercise the authorization resolution path. */
  public static class DeniedTableCatalog implements TableCatalog {

    private String name;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Table loadTable(Identifier ident) {
      String tableIdentifier =
          name + "." + String.join(".", ident.namespace()) + "." + ident.name();
      return AuthorizationTable.deny(
          ident.name(),
          tableIdentifier,
          ImmutableSet.of(Privilege.Name.SELECT_TABLE),
          new ForbiddenException("denied %s", tableIdentifier));
    }

    @Override
    public Identifier[] listTables(String[] namespace) {
      return new Identifier[0];
    }

    @Override
    public Table createTable(
        Identifier ident, StructType schema, Transform[] partitions, Map<String, String> props) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropTable(Identifier ident) {
      return false;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {
      throw new UnsupportedOperationException();
    }
  }
}
