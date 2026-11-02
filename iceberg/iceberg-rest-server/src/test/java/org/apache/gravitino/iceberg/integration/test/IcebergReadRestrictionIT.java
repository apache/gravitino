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

import com.google.common.collect.ImmutableList;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
    catalogClientWithAllPrivilege.asSchemas().createSchema(SCHEMA_NAME, null, new HashMap<>());
  }

  @Override
  protected TestDatabaseName getTestDatabaseName() {
    return TestDatabaseName.PG_ICEBERG_READ_RESTRICTION_IT;
  }

  @Test
  void testSparkEnforcesReadRestrictions() {
    String tableName = "read_restrictions";
    createTable(SCHEMA_NAME, tableName);
    grantTablePrivileges(tableName, Privileges.SelectTable.allow(), Privileges.ModifyTable.allow());
    sql("USE %s", SPARK_CATALOG_NAME);
    sql("USE %s", SCHEMA_NAME);

    sql("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName);
    List<Object[]> unrestrictedRows = sql("SELECT col_1, col_2 FROM %s ORDER BY col_1", tableName);
    Assertions.assertEquals(2, unrestrictedRows.size());

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

  private void grantTablePrivileges(String tableName, Privilege... privileges) {
    String roleName = "readRestrictionRole_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, SCHEMA_NAME, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);
    securableObjects.add(
        SecurableObjects.ofTable(schemaObject, tableName, ImmutableList.copyOf(privileges)));
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }
}
