/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OwnerAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String hmsUri;
  private static final String role = "role";
  private static final String TEMP_USER = "user3";
  private static ShellJobTemplate.Builder builder;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startHiveContainer();
    super.startIntegrationTest();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    client
        .loadMetalake(METALAKE)
        .createCatalog(CATALOG, Catalog.Type.RELATIONAL, "hive", "comment", properties)
        .asSchemas()
        .createSchema(SCHEMA, "test", new HashMap<>());
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.createRole(role, new HashMap<>(), Collections.emptyList());
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(role), NORMAL_USER);
    client.loadMetalake(METALAKE).addUser(TEMP_USER);
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of(SCHEMA, "table1"),
        new Column[] {Column.of("col1", Types.StringType.get())},
        "test",
        new HashMap<>());
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    client.loadMetalake(METALAKE).loadCatalog(CATALOG).asSchemas().dropSchema(SCHEMA, true);
    super.stopIntegrationTest();
  }

  @Test
  @Order(1)
  public void testSetTableOwnerByMetalakeOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
  }

  @Test
  @Order(2)
  public void testSetTableOwnerByCatalogOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
    // set catalog owner to normal user
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        NORMAL_USER,
        Owner.Type.USER);
    // normal user can set owner
    gravitinoMetalakeLoadByNormalUser.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);

    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
  }

  @Test
  @Order(3)
  public void testSetTableOwnerBySchemaOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
    // set schema owner to normal user
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);
    // normal user can set owner
    assertThrows(
        "Current user can not set owner",
        ForbiddenException.class,
        () -> {
          // NORMAL_USER has not USE_CATALOG
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        ImmutableList.of(Privileges.UseCatalog.allow()));
    gravitinoMetalakeLoadByNormalUser.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);

    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
  }

  @Test
  @Order(4)
  public void testSetTableOwnerByTableOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
    // set schema owner to normal user
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    assertThrows(
        "Current user can not set owner",
        ForbiddenException.class,
        () -> {
          // NORMAL_USER has not USE_SCHEMA
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow()));
    gravitinoMetalakeLoadByNormalUser.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);

    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
  }

  @Test
  @Order(5)
  public void testSetRoleOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    String tempRole = "tempRole";
    String tempUser = "tempUser";
    gravitinoMetalake.addUser(tempUser);
    gravitinoMetalake.createRole(tempRole, new HashMap<>(), Collections.emptyList());
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(ImmutableList.of(tempRole), MetadataObject.Type.ROLE),
              tempUser,
              Owner.Type.USER);
        });
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(tempRole), MetadataObject.Type.ROLE),
        tempUser,
        Owner.Type.USER);
    // reset
    gravitinoMetalake.deleteRole(tempRole);
  }

  @Test
  @Order(6)
  public void testGetCatalogOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.getOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG));
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    gravitinoMetalake.revokePrivilegesFromRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        ImmutableSet.of(Privileges.UseCatalog.allow()));
    assertThrows(
        "Current user can not get owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getOwner(
              MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG));
        });
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        ImmutableSet.of(Privileges.UseCatalog.allow()));
  }

  @Test
  @Order(7)
  public void testGetSchemaOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.getOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA));
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    gravitinoMetalakeLoadByNormalUser.getOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA));
    gravitinoMetalake.revokePrivilegesFromRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        ImmutableSet.of(Privileges.UseSchema.allow()));
    assertThrows(
        "Current user can not get owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getOwner(
              MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA));
        });
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        ImmutableSet.of(Privileges.UseSchema.allow()));
  }

  @Test
  @Order(8)
  public void testGetTableOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.getOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE));
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not get owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE));
        });
  }

  @Test
  @Order(9)
  public void testGetMetalakeOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.getOwner(
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE));
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    gravitinoMetalakeLoadByNormalUser.getOwner(
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE));
    client.createMetalake("tempMetalake", "metalake1 comment", Collections.emptyMap());
  }

  @Test
  @Order(10)
  public void testGetPolicyOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    Set<MetadataObject.Type> supportedTypes = ImmutableSet.of(MetadataObject.Type.TABLE);
    String policyName = "policy1";
    gravitinoMetalake.createPolicy(
        policyName, "custom", "policy1", true, PolicyContents.custom(null, supportedTypes, null));
    gravitinoMetalake.getOwner(
        MetadataObjects.of(ImmutableList.of(policyName), MetadataObject.Type.POLICY));
    assertThrows(
        "Current user can not get owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getOwner(
              MetadataObjects.of(ImmutableList.of(policyName), MetadataObject.Type.POLICY));
        });
  }

  @Test
  @Order(11)
  public void testGetTagOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    String tagName = "tag1";
    gravitinoMetalake.createTag("tag1", "tag1", Map.of());
    gravitinoMetalake.getOwner(
        MetadataObjects.of(ImmutableList.of(tagName), MetadataObject.Type.TAG));
    assertThrows(
        "Current user can not get owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getOwner(
              MetadataObjects.of(ImmutableList.of(tagName), MetadataObject.Type.TAG));
        });
  }

  @Test
  @Order(12)
  public void testGetJobOwner() throws IOException {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    testStagingDir = Files.createTempDirectory("test_staging_dir").toFile();
    String testEntryScriptPath = generateTestEntryScript();
    String testLibScriptPath = generateTestLibScript();

    builder =
        ShellJobTemplate.builder()
            .withComment("Test shell job template")
            .withExecutable(testEntryScriptPath)
            .withArguments(Lists.newArrayList("{{arg1}}", "{{arg2}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "{{env_var}}"))
            .withScripts(Lists.newArrayList(testLibScriptPath))
            .withCustomFields(Collections.emptyMap());
    JobTemplate template1 = builder.withName("test_1").build();
    gravitinoMetalake.registerJobTemplate(template1);
    gravitinoMetalake.getOwner(
        MetadataObjects.of(ImmutableList.of("test_1"), MetadataObject.Type.JOB_TEMPLATE));
    assertThrows(
        "Current user can not get owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getOwner(
              MetadataObjects.of(ImmutableList.of("test_1"), MetadataObject.Type.JOB_TEMPLATE));
        });
    JobHandle jobHandle =
        gravitinoMetalake.runJob(
            "test_1", ImmutableMap.of("arg1", "value1", "arg2", "success", "env_var", "value2"));
    gravitinoMetalake.getOwner(
        MetadataObjects.of(ImmutableList.of(jobHandle.jobId()), MetadataObject.Type.JOB));
    assertThrows(
        "Current user can not get owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getOwner(
              MetadataObjects.of(ImmutableList.of(jobHandle.jobId()), MetadataObject.Type.JOB));
        });
  }

  @Test
  @Order(13)
  public void getRoleOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    gravitinoMetalakeLoadByNormalUser.getOwner(
        MetadataObjects.of(ImmutableList.of(role), MetadataObject.Type.ROLE));
    gravitinoMetalake.revokeRolesFromUser(ImmutableList.of(role), NORMAL_USER);
    assertThrows(
        "Current user can not get owner",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getOwner(
              MetadataObjects.of(ImmutableList.of(role), MetadataObject.Type.ROLE));
        });
  }
}
