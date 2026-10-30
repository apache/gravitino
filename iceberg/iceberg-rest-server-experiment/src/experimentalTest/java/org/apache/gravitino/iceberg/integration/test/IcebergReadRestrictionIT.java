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
package org.apache.gravitino.iceberg.integration.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.iceberg.integration.test.auth.ReadRestrictionGroupAuthenticator;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.tag.TagValueConstraint;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies that Spark enforces read restrictions returned by the experimental Iceberg service. */
@Tag("gravitino-docker-test")
public class IcebergReadRestrictionIT extends IcebergAuthorizationIT {

  private static final String EXPERIMENTAL_SERVER_DIRECTORY = "iceberg-rest-server-experiment";
  private static final String SCHEMA_NAME = "schema";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    String rootDirectory = System.getenv("GRAVITINO_ROOT_DIR");
    String experimentalServerHome =
        Paths.get(rootDirectory, "iceberg", EXPERIMENTAL_SERVER_DIRECTORY).toString();
    customConfigs.put(
        "gravitino.iceberg-rest.classpath",
        String.join(
            ",",
            Paths.get(experimentalServerHome, "build", "libs").toString(),
            Paths.get(experimentalServerHome, "build", "dependencies").toString()));
    super.startIntegrationTest();
    // Limit retries when table loads are expected to fail.
    sql("SET `spark.sql.catalog.%s.rest.client.max-retries`=1", SPARK_CATALOG_NAME);
    metalakeClientWithAllPrivilege.addGroup(ReadRestrictionGroupAuthenticator.AUDITORS_GROUP);
    catalogClientWithAllPrivilege.asSchemas().createSchema(SCHEMA_NAME, null, new HashMap<>());
  }

  /** {@inheritDoc} */
  @Override
  protected String authenticator() {
    // BaseIT requires the "simple" entry to create its Basic-auth admin client. The fixture
    // handles all Basic tokens; the second authenticator is only a client-setup hint to BaseIT.
    return ReadRestrictionGroupAuthenticator.class.getCanonicalName() + ",simple";
  }

  @Override
  protected TestDatabaseName getTestDatabaseName() {
    return TestDatabaseName.PG_ICEBERG_READ_RESTRICTION_IT;
  }

  @BeforeEach
  void resetUserRoles() {
    revokeUserRoles();
  }

  @Test
  void testSparkEnforcesReadRestrictions() throws Exception {
    String tableName = "read_restrictions";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);

    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);
    List<Object[]> unrestrictedRows = sql("SELECT col_1, col_2 FROM %s ORDER BY col_1", tableName);
    Assertions.assertEquals(2, unrestrictedRows.size());

    Assertions.assertTrue(
        loadTableResponseAs(SCHEMA_NAME, tableName, NORMAL_USER)
            .path("read-restrictions")
            .isMissingNode());

    String suffix = UUID.randomUUID().toString().replace("-", "");
    String rowFilterTag = "row_filter_tag_" + suffix;
    String rowFilterPolicy = "row_filter_policy_" + suffix;
    metalakeClientWithAllPrivilege.createTag(rowFilterTag, null, new HashMap<>());
    metalakeClientWithAllPrivilege.createPolicy(
        rowFilterPolicy,
        Policy.BuiltInType.ROW_FILTER.policyType(),
        null,
        true,
        PolicyContents.rowFilter("(col_1 INTEGER) RETURNS BOOLEAN -> col_1 = 1"));
    metalakeClientWithAllPrivilege.addPolicyForTag(rowFilterTag, rowFilterPolicy);

    String columnMaskTag = "column_mask_tag_" + suffix;
    String columnMaskPolicy = "column_mask_policy_" + suffix;
    metalakeClientWithAllPrivilege.createTag(columnMaskTag, null, new HashMap<>());
    metalakeClientWithAllPrivilege.createPolicy(
        columnMaskPolicy,
        Policy.BuiltInType.COLUMN_MASK.policyType(),
        null,
        true,
        PolicyContents.columnMask("(value INTEGER) RETURNS INTEGER -> replace_with_null(value)"));
    metalakeClientWithAllPrivilege.addPolicyForTag(columnMaskTag, columnMaskPolicy);

    Table gravitinoTable =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA_NAME, tableName));
    gravitinoTable.supportsTags().associateTags(new String[] {rowFilterTag}, null);
    gravitinoTable.columns()[1].supportsTags().associateTags(new String[] {columnMaskTag}, null);

    List<Object[]> restrictedRows = sql("SELECT col_1, col_2 FROM %s ORDER BY col_1", tableName);
    Assertions.assertEquals(1, restrictedRows.size());
    Assertions.assertArrayEquals(new Object[] {1, null}, restrictedRows.get(0));
  }

  @Test
  void testRowFilterFollowsPolicyAndTagChanges() throws Exception {
    String tableName = "row_filter_lifecycle";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);

    PolicyTag policyTag =
        createPolicyAndTag(
            Policy.BuiltInType.ROW_FILTER, "(col_1 INTEGER) RETURNS BOOLEAN -> col_1 = 1");
    String tag = policyTag.tagName;
    Table table = loadTable(tableName);
    table.supportsTags().associateTags(new String[] {tag}, null);
    assertRows(tableName, new Object[] {1, 10});

    String policy = policyTag.policyName;
    metalakeClientWithAllPrivilege.disablePolicy(policy);
    assertRows(tableName, new Object[] {1, 10}, new Object[] {2, 20});

    metalakeClientWithAllPrivilege.enablePolicy(policy);
    assertRows(tableName, new Object[] {1, 10});

    table.supportsTags().associateTags(null, new String[] {tag});
    assertRows(tableName, new Object[] {1, 10}, new Object[] {2, 20});
    Assertions.assertTrue(
        loadTableResponseAs(SCHEMA_NAME, tableName, NORMAL_USER)
            .path("read-restrictions")
            .isMissingNode());
    table.supportsTags().associateTags(new String[] {tag}, null);
    assertRows(tableName, new Object[] {1, 10});
    Assertions.assertTrue(metalakeClientWithAllPrivilege.deleteTag(tag));
    assertRows(tableName, new Object[] {1, 10}, new Object[] {2, 20});
    Assertions.assertTrue(
        loadTableResponseAs(SCHEMA_NAME, tableName, NORMAL_USER)
            .path("read-restrictions")
            .isMissingNode());
  }

  @Test
  void testRowFilterBindsAuthenticatedUser() {
    String tableName = "row_filter_user";
    catalogClientWithAllPrivilege
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SCHEMA_NAME, tableName),
            new Column[] {
              Column.of("owner", Types.StringType.get(), "owner"),
              Column.of("value", Types.IntegerType.get(), "value")
            },
            "table_comment",
            new HashMap<>());
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES ('normal', 10), ('other', 20)", tableName);

    String tag =
        createPolicyTag(
            Policy.BuiltInType.ROW_FILTER,
            "(owner STRING) RETURNS BOOLEAN -> owner = CURRENT_USER()");
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    List<Object[]> rows = sql("SELECT owner, value FROM %s ORDER BY owner", tableName);
    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(new Object[] {NORMAL_USER, 10}, rows.get(0));
  }

  @Test
  void testReadRestrictionsBindEachRequestUser() throws Exception {
    String tableName = "request_user_binding";
    catalogClientWithAllPrivilege
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SCHEMA_NAME, tableName),
            new Column[] {Column.of("owner", Types.StringType.get(), "owner")},
            "table_comment",
            new HashMap<>());
    grantTablePrivileges(tableName, Privileges.SelectTable.allow());
    String tag =
        createPolicyTag(
            Policy.BuiltInType.ROW_FILTER,
            "(owner STRING) RETURNS BOOLEAN -> owner = CURRENT_USER()");
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    String normalRestrictions = loadRestrictionsAs(tableName, NORMAL_USER);
    String superRestrictions = loadRestrictionsAs(tableName, SUPER_USER);
    Assertions.assertTrue(normalRestrictions.contains(NORMAL_USER));
    Assertions.assertTrue(superRestrictions.contains(SUPER_USER));
    Assertions.assertNotEquals(normalRestrictions, superRestrictions);
  }

  @Test
  void testKnownGroupNonmemberSelectsFallback() {
    String tableName = "known_group_nonmember";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);

    String group = "restriction_group_" + UUID.randomUUID().toString().replace("-", "");
    metalakeClientWithAllPrivilege.addGroup(group);
    String expression =
        "(col_1 INTEGER) RETURNS BOOLEAN -> CASE WHEN is_group_member('"
            + group
            + "') THEN col_1 = 1 ELSE col_1 = 2 END";
    String tag = createPolicyTag(Policy.BuiltInType.ROW_FILTER, expression);
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    assertRows(tableName, new Object[] {2, 20});
  }

  @Test
  void testGroupMemberSelectsProtectedBranch() throws Exception {
    String tableName = "group_member_filter";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);

    String expression =
        "(col_1 INTEGER) RETURNS BOOLEAN -> CASE WHEN is_group_member('"
            + ReadRestrictionGroupAuthenticator.AUDITORS_GROUP
            + "') THEN col_1 = 1 ELSE col_1 = 2 END";
    String tag = createPolicyTag(Policy.BuiltInType.ROW_FILTER, expression);
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    assertRows(tableName, new Object[] {1, 10});
    Assertions.assertNotEquals(
        loadRestrictionsAs(tableName, NORMAL_USER), loadRestrictionsAs(tableName, SUPER_USER));
  }

  @Test
  void testComplexRowFilterExpression() {
    String tableName = "complex_row_filter";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30), (4, 40)", tableName);

    String expression =
        "(col_1 INTEGER, col_2 INTEGER) RETURNS BOOLEAN -> "
            + "(col_1 IN (1, 3) AND col_2 >= 20) OR (col_1 = 4 AND NOT (col_2 = 10))";
    String tag = createPolicyTag(Policy.BuiltInType.ROW_FILTER, expression);
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    assertRows(tableName, new Object[] {3, 30}, new Object[] {4, 40});
  }

  @Test
  void testConditionalRowFilterSelectsMatchingBranch() {
    String tableName = "conditional_row_filter";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30)", tableName);

    String expression =
        "(col_1 INTEGER) RETURNS BOOLEAN -> CASE "
            + "WHEN CURRENT_USER() = 'super' THEN col_1 = 1 "
            + "WHEN CURRENT_USER() = 'normal' THEN col_1 = 2 ELSE FALSE END";
    String tag = createPolicyTag(Policy.BuiltInType.ROW_FILTER, expression);
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    assertRows(tableName, new Object[] {2, 20});
  }

  @Test
  void testInvalidUnselectedBranchFailsClosed() {
    String tableName = "invalid_unselected_branch";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);

    String expression =
        "(missing_column INTEGER) RETURNS BOOLEAN -> CASE "
            + "WHEN CURRENT_USER() = 'normal' THEN TRUE ELSE missing_column = 1 END";
    String tag = createPolicyTag(Policy.BuiltInType.ROW_FILTER, expression);
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    Assertions.assertThrows(
        RuntimeException.class, () -> sql("SELECT col_1, col_2 FROM %s", tableName));
  }

  @Test
  void testDuplicateRowFilterIsAppliedOnce() {
    String tableName = "duplicate_row_filter";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);

    PolicyTag policyTag =
        createPolicyAndTag(
            Policy.BuiltInType.ROW_FILTER, "(col_1 INTEGER) RETURNS BOOLEAN -> col_1 = 1");
    String firstTag = policyTag.tagName;
    String secondTag = "duplicate_tag_" + UUID.randomUUID().toString().replace("-", "");
    metalakeClientWithAllPrivilege.createTag(secondTag, null, new HashMap<>());
    String policy = policyTag.policyName;
    metalakeClientWithAllPrivilege.addPolicyForTag(secondTag, policy);
    loadTable(tableName).supportsTags().associateTags(new String[] {firstTag, secondTag}, null);

    assertRows(tableName, new Object[] {1, 10});
  }

  @Test
  void testEquivalentComplexRowFiltersDoNotConflict() {
    String tableName = "equivalent_row_filters";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30)", tableName);

    String firstTag =
        createPolicyTag(
            Policy.BuiltInType.ROW_FILTER,
            "(col_1 INTEGER, col_2 INTEGER) RETURNS BOOLEAN -> col_1 IN (2, 1, 2) AND col_2 >= 10");
    String secondTag =
        createPolicyTag(
            Policy.BuiltInType.ROW_FILTER,
            "(col_1 INTEGER, col_2 INTEGER) RETURNS BOOLEAN -> col_2 >= 10 AND col_1 IN (1, 2)");
    loadTable(tableName).supportsTags().associateTags(new String[] {firstTag, secondTag}, null);

    assertRows(tableName, new Object[] {1, 10}, new Object[] {2, 20});
  }

  @Test
  void testUnknownGroupInConditionFailsClosed() {
    String tableName = "unknown_group_filter";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);

    String expression =
        "(col_1 INTEGER) RETURNS BOOLEAN -> CASE WHEN is_group_member('missing_group') "
            + "THEN col_1 = 1 ELSE col_1 = 2 END";
    String tag = createPolicyTag(Policy.BuiltInType.ROW_FILTER, expression);
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    Assertions.assertThrows(
        RuntimeException.class, () -> sql("SELECT col_1, col_2 FROM %s", tableName));
  }

  @Test
  void testConflictingRowFiltersFailClosed() {
    String tableName = "conflicting_row_filters";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);

    String firstTag =
        createPolicyTag(
            Policy.BuiltInType.ROW_FILTER, "(col_1 INTEGER) RETURNS BOOLEAN -> col_1 = 1");
    String secondTag =
        createPolicyTag(
            Policy.BuiltInType.ROW_FILTER, "(col_1 INTEGER) RETURNS BOOLEAN -> col_1 = 2");
    Table table = loadTable(tableName);
    table.supportsTags().associateTags(new String[] {firstTag, secondTag}, null);

    Assertions.assertThrows(
        RuntimeException.class, () -> sql("SELECT col_1, col_2 FROM %s", tableName));

    table.supportsTags().associateTags(null, new String[] {secondTag});
    assertRows(tableName, new Object[] {1, 10});
  }

  @Test
  void testInheritedTagValueSelectsPolicyAndTableValueOverridesIt() throws Exception {
    String schemaName = "inherited_restrictions";
    String inheritedTable = "inherited_filter";
    String overriddenTable = "overridden_filter";
    catalogClientWithAllPrivilege.asSchemas().createSchema(schemaName, null, new HashMap<>());
    createTable(schemaName, inheritedTable);
    createTable(schemaName, overriddenTable);
    grantTablePrivileges(
        schemaName, inheritedTable, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    grantTablePrivileges(
        schemaName,
        overriddenTable,
        Privileges.SelectTable.allow(),
        Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", schemaName);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", inheritedTable);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", overriddenTable);

    String suffix = UUID.randomUUID().toString().replace("-", "");
    String tag = "inherited_tag_" + suffix;
    String policy = "inherited_policy_" + suffix;
    metalakeClientWithAllPrivilege.createTag(
        tag, null, new HashMap<>(), TagValueConstraint.ofAllowedValues("finance", "risk"));
    metalakeClientWithAllPrivilege.createPolicy(
        policy,
        Policy.BuiltInType.ROW_FILTER.policyType(),
        null,
        true,
        PolicyContents.rowFilter("(col_1 INTEGER) RETURNS BOOLEAN -> col_1 = 1"));
    metalakeClientWithAllPrivilege.addPolicyForTag(tag, policy, TagValueSelector.of("finance"));
    catalogClientWithAllPrivilege
        .asSchemas()
        .loadSchema(schemaName)
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of(tag, "finance")}, null);
    loadTable(schemaName, overriddenTable)
        .supportsTags()
        .associateTags(new TagValue[] {TagValue.of(tag, "risk")}, null);

    Assertions.assertFalse(loadRestrictionsAs(schemaName, inheritedTable, NORMAL_USER).isEmpty());
    Assertions.assertTrue(
        loadTableResponseAs(schemaName, overriddenTable, NORMAL_USER)
            .path("read-restrictions")
            .isMissingNode());
    assertRows(inheritedTable, new Object[] {1, 10});
    assertRows(overriddenTable, new Object[] {1, 10}, new Object[] {2, 20});

    loadTable(schemaName, overriddenTable)
        .supportsTags()
        .associateTags(
            new TagValue[] {TagValue.of(tag, "finance")},
            new TagValue[] {TagValue.of(tag, "risk")});
    assertRows(overriddenTable, new Object[] {1, 10});
  }

  @Test
  void testCatalogSchemaAndTableTagValuesFallBackInOrder() {
    String schemaName = "tag_value_precedence";
    String tableName = "tag_value_precedence_table";
    catalogClientWithAllPrivilege.asSchemas().createSchema(schemaName, null, new HashMap<>());
    createTable(schemaName, tableName);
    grantTablePrivileges(
        schemaName, tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", schemaName);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);

    String suffix = UUID.randomUUID().toString().replace("-", "");
    String tag = "precedence_tag_" + suffix;
    String policy = "precedence_policy_" + suffix;
    metalakeClientWithAllPrivilege.createTag(
        tag, null, new HashMap<>(), TagValueConstraint.ofAllowedValues("finance", "risk"));
    metalakeClientWithAllPrivilege.createPolicy(
        policy,
        Policy.BuiltInType.ROW_FILTER.policyType(),
        null,
        true,
        PolicyContents.rowFilter("(col_1 INTEGER) RETURNS BOOLEAN -> col_1 = 1"));
    metalakeClientWithAllPrivilege.addPolicyForTag(tag, policy, TagValueSelector.of("finance"));
    TagValue finance = TagValue.of(tag, "finance");
    TagValue risk = TagValue.of(tag, "risk");
    catalogClientWithAllPrivilege.supportsTags().associateTags(new TagValue[] {risk}, null);
    try {
      assertRows(tableName, new Object[] {1, 10}, new Object[] {2, 20});

      catalogClientWithAllPrivilege
          .asSchemas()
          .loadSchema(schemaName)
          .supportsTags()
          .associateTags(new TagValue[] {finance}, null);
      assertRows(tableName, new Object[] {1, 10});

      loadTable(schemaName, tableName).supportsTags().associateTags(new TagValue[] {risk}, null);
      assertRows(tableName, new Object[] {1, 10}, new Object[] {2, 20});

      loadTable(schemaName, tableName).supportsTags().associateTags(null, new TagValue[] {risk});
      assertRows(tableName, new Object[] {1, 10});

      catalogClientWithAllPrivilege
          .asSchemas()
          .loadSchema(schemaName)
          .supportsTags()
          .associateTags(null, new TagValue[] {finance});
      assertRows(tableName, new Object[] {1, 10}, new Object[] {2, 20});
    } finally {
      catalogClientWithAllPrivilege.supportsTags().associateTags(null, new TagValue[] {risk});
    }
  }

  @Test
  void testColumnMaskFollowsTagChanges() {
    String tableName = "column_mask_lifecycle";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10)", tableName);

    String tag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK,
            "(value INTEGER) RETURNS INTEGER -> replace_with_null(value)");
    Table table = loadTable(tableName);
    table.columns()[1].supportsTags().associateTags(new String[] {tag}, null);
    assertRows(tableName, new Object[] {1, null});

    table.columns()[1].supportsTags().associateTags(null, new String[] {tag});
    assertRows(tableName, new Object[] {1, 10});
  }

  @Test
  void testConflictingColumnMasksFailClosed() {
    String tableName = "conflicting_column_masks";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);

    String nullTag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK,
            "(value INTEGER) RETURNS INTEGER -> replace_with_null(value)");
    String fixedTag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK,
            "(value INTEGER) RETURNS INTEGER -> mask_to_fixed_value(value)");
    Table table = loadTable(tableName);
    table.columns()[1].supportsTags().associateTags(new String[] {nullTag, fixedTag}, null);

    Assertions.assertThrows(
        RuntimeException.class, () -> sql("SELECT col_1, col_2 FROM %s", tableName));
  }

  @Test
  void testConditionalMasksApplyToMultipleColumns() throws Exception {
    String tableName = "conditional_column_masks";
    catalogClientWithAllPrivilege
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SCHEMA_NAME, tableName),
            new Column[] {
              Column.of("id", Types.IntegerType.get(), "id"),
              Column.of("account", Types.StringType.get(), "account"),
              Column.of("note", Types.StringType.get(), "note")
            },
            "table_comment",
            new HashMap<>());
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, '12345678', 'abcdefgh')", tableName);

    String accountTag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK,
            "(value STRING) RETURNS STRING -> CASE WHEN CURRENT_USER() = 'normal' "
                + "THEN replace_with_null(value) ELSE show_last_4(value) END");
    String noteTag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK,
            "(value STRING) RETURNS STRING -> replace_with_null(value)");
    Table table = loadTable(tableName);
    table.columns()[1].supportsTags().associateTags(new String[] {accountTag}, null);
    table.columns()[2].supportsTags().associateTags(new String[] {noteTag}, null);

    List<Object[]> rows = sql("SELECT id, account, note FROM %s", tableName);
    Assertions.assertEquals(1, rows.size());
    Assertions.assertArrayEquals(new Object[] {1, null, null}, rows.get(0));

    String normalRestrictions = loadRestrictionsAs(tableName, NORMAL_USER);
    String superRestrictions = loadRestrictionsAs(tableName, SUPER_USER);
    Assertions.assertFalse(normalRestrictions.contains("show-last-4"));
    Assertions.assertTrue(superRestrictions.contains("show-last-4"));
  }

  @Test
  void testSparkAppliesFirstAndLastFourMasks() {
    String tableName = "string_mask_actions";
    catalogClientWithAllPrivilege
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SCHEMA_NAME, tableName),
            new Column[] {
              Column.of("id", Types.IntegerType.get(), "id"),
              Column.of("account", Types.StringType.get(), "account"),
              Column.of("note", Types.StringType.get(), "note")
            },
            "table_comment",
            new HashMap<>());
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, '12345678', 'abcdefgh')", tableName);

    String lastTag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK, "(value STRING) RETURNS STRING -> show_last_4(value)");
    String firstTag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK, "(value STRING) RETURNS STRING -> show_first_4(value)");
    Table table = loadTable(tableName);
    table.columns()[1].supportsTags().associateTags(new String[] {lastTag}, null);
    table.columns()[2].supportsTags().associateTags(new String[] {firstTag}, null);

    List<Object[]> rows = sql("SELECT id, account, note FROM %s", tableName);
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1, rows.get(0)[0]);
    String account = (String) rows.get(0)[1];
    String note = (String) rows.get(0)[2];
    Assertions.assertNotEquals("12345678", account);
    Assertions.assertNotEquals("abcdefgh", note);
    Assertions.assertTrue(account.endsWith("5678"));
    Assertions.assertTrue(note.startsWith("abcd"));
  }

  @ParameterizedTest
  @MethodSource("sqlNullPredicates")
  void testSparkEnforcesSqlNullAndCaseSemantics(String predicate, List<Integer> expectedIds) {
    String tableName = "sql_nulls_" + UUID.randomUUID().toString().replace("-", "");
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, NULL), (4, 40)", tableName);

    String tag =
        createPolicyTag(
            Policy.BuiltInType.ROW_FILTER, "(col_2 INTEGER) RETURNS BOOLEAN -> " + predicate);
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    List<Object[]> rows = sql("SELECT col_1 FROM %s ORDER BY col_1", tableName);
    List<Integer> actualIds = new ArrayList<>();
    for (Object[] row : rows) {
      actualIds.add((Integer) row[0]);
    }
    Assertions.assertEquals(expectedIds, actualIds, predicate);
  }

  @Test
  void testMatchedCaseSkipsLaterGroupLookup() {
    String tableName = "case_skips_group";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);

    String tag =
        createPolicyTag(
            Policy.BuiltInType.ROW_FILTER,
            "(col_1 INTEGER) RETURNS BOOLEAN -> CASE "
                + "WHEN CURRENT_USER() = 'normal' THEN col_1 = 1 "
                + "WHEN is_group_member('missing_group') THEN col_1 = 2 ELSE FALSE END");
    loadTable(tableName).supportsTags().associateTags(new String[] {tag}, null);

    assertRows(tableName, new Object[] {1, 10});
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CASE WHEN CURRENT_USER() = 'normal' THEN value ELSE NULL END",
        "CASE WHEN CURRENT_USER() = 'normal' THEN value END"
      })
  void testNullableCaseMaskFailsForRequiredColumn(String expression) {
    String tableName = "required_mask_" + UUID.randomUUID().toString().replace("-", "");
    catalogClientWithAllPrivilege
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(SCHEMA_NAME, tableName),
            new Column[] {
              Column.of("col_1", Types.IntegerType.get()),
              Column.of(
                  "col_2",
                  Types.IntegerType.get(),
                  null,
                  false,
                  false,
                  Column.DEFAULT_VALUE_NOT_SET)
            },
            "table_comment",
            new HashMap<>());
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10)", tableName);

    String tag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK, "(value INTEGER) RETURNS INTEGER -> " + expression);
    Table table = loadTable(tableName);
    Assertions.assertFalse(table.columns()[1].nullable());
    table.columns()[1].supportsTags().associateTags(new String[] {tag}, null);

    Assertions.assertThrows(
        RuntimeException.class, () -> sql("SELECT col_1, col_2 FROM %s", tableName));
  }

  @Test
  void testRowDependentMaskSelectionFailsClosed() {
    String tableName = "row_dependent_mask";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);
    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);

    String tag =
        createPolicyTag(
            Policy.BuiltInType.COLUMN_MASK,
            "(value INTEGER) RETURNS INTEGER -> CASE WHEN value = 10 "
                + "THEN mask_to_fixed_value(value) ELSE value END");
    loadTable(tableName).columns()[1].supportsTags().associateTags(new String[] {tag}, null);

    Assertions.assertThrows(
        RuntimeException.class, () -> sql("SELECT col_1, col_2 FROM %s", tableName));
  }

  private static Stream<Arguments> sqlNullPredicates() {
    return Stream.of(
        Arguments.of("NOT (col_2 = 20)", ImmutableList.of(1, 4)),
        Arguments.of("col_2 NOT IN (20, NULL)", ImmutableList.of()),
        Arguments.of("col_2 IN (20, NULL)", ImmutableList.of(2)),
        Arguments.of("col_2 IS NULL OR col_2 NOT BETWEEN 20 AND 30", ImmutableList.of(1, 3, 4)),
        Arguments.of(
            "CASE WHEN col_2 = NULL THEN TRUE WHEN col_2 = 20 THEN TRUE ELSE FALSE END",
            ImmutableList.of(2)),
        Arguments.of("CASE col_2 WHEN NULL THEN TRUE WHEN 20 THEN TRUE END", ImmutableList.of(2)),
        Arguments.of("CASE WHEN col_2 = 20 THEN TRUE END", ImmutableList.of(2)));
  }

  private String createPolicyTag(Policy.BuiltInType type, String expression) {
    return createPolicyAndTag(type, expression).tagName;
  }

  private PolicyTag createPolicyAndTag(Policy.BuiltInType type, String expression) {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    String tag = "restriction_tag_" + suffix;
    String policy = "restriction_policy_" + suffix;
    metalakeClientWithAllPrivilege.createTag(tag, null, new HashMap<>());
    metalakeClientWithAllPrivilege.createPolicy(
        policy,
        type.policyType(),
        null,
        true,
        type == Policy.BuiltInType.ROW_FILTER
            ? PolicyContents.rowFilter(expression)
            : PolicyContents.columnMask(expression));
    metalakeClientWithAllPrivilege.addPolicyForTag(tag, policy);
    return new PolicyTag(tag, policy);
  }

  private Table loadTable(String tableName) {
    return loadTable(SCHEMA_NAME, tableName);
  }

  private Table loadTable(String schemaName, String tableName) {
    return catalogClientWithAllPrivilege
        .asTableCatalog()
        .loadTable(NameIdentifier.of(schemaName, tableName));
  }

  private void assertRows(String tableName, Object[]... expectedRows) {
    List<Object[]> rows = sql("SELECT col_1, col_2 FROM %s ORDER BY col_1", tableName);
    Assertions.assertEquals(expectedRows.length, rows.size());
    for (int index = 0; index < expectedRows.length; index++) {
      Assertions.assertArrayEquals(expectedRows[index], rows.get(index));
    }
  }

  private String loadRestrictionsAs(String tableName, String userName) throws Exception {
    return loadRestrictionsAs(SCHEMA_NAME, tableName, userName);
  }

  private String loadRestrictionsAs(String schemaName, String tableName, String userName)
      throws Exception {
    JsonNode response = loadTableResponseAs(schemaName, tableName, userName);
    JsonNode restrictions = response.path("read-restrictions");
    Assertions.assertFalse(restrictions.isMissingNode(), response.toString());
    return restrictions.toString();
  }

  private JsonNode loadTableResponseAs(String schemaName, String tableName, String userName)
      throws Exception {
    String uri =
        getIcebergRestServiceUri()
            + "v1/"
            + GRAVITINO_CATALOG_NAME
            + "/namespaces/"
            + schemaName
            + "/tables/"
            + tableName;
    String credentials = userName + ":mock";
    String authorization =
        "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(uri))
            .header("Authorization", authorization)
            .GET()
            .build();
    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    Assertions.assertEquals(200, response.statusCode(), response.body());
    return new ObjectMapper().readTree(response.body());
  }

  private void grantTablePrivileges(String tableName, Privilege... privileges) {
    grantTablePrivileges(SCHEMA_NAME, tableName, privileges);
  }

  private void grantTablePrivileges(String schemaName, String tableName, Privilege... privileges) {
    String roleName = "readRestrictionRole_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schemaName, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);
    securableObjects.add(
        SecurableObjects.ofTable(schemaObject, tableName, ImmutableList.copyOf(privileges)));
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }

  private static final class PolicyTag {
    private final String tagName;
    private final String policyName;

    private PolicyTag(String tagName, String policyName) {
      this.tagName = tagName;
      this.policyName = policyName;
    }
  }
}
