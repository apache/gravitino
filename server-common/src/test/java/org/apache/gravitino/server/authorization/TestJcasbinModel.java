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

package org.apache.gravitino.server.authorization;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.model.Model;
import org.casbin.jcasbin.persist.file_adapter.FileAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Test for jcasbin model. */
public class TestJcasbinModel {

  /** Jcasbin enforcer */
  private static Enforcer enforcer;

  @BeforeAll
  public static void init() throws IOException {
    // Load jcasbin policy from jcasbin_policy.txt
    URL resource = TestJcasbinModel.class.getResource("/jcasbin_policy.txt");
    Assertions.assertNotNull(resource);
    List<String> policyWithLicense =
        FileUtils.readLines(new File(resource.getFile()), StandardCharsets.UTF_8);
    // remove license
    String policy =
        policyWithLicense.stream()
            .filter(line -> !line.startsWith("#"))
            .collect(Collectors.joining("\n"));
    try (InputStream modelStream =
            TestJcasbinModel.class.getResourceAsStream("/jcasbin_model.conf");
        InputStream policyInputStream =
            new ByteArrayInputStream(policy.getBytes(StandardCharsets.UTF_8))) {
      Assertions.assertNotNull(modelStream);
      String modelString = IOUtils.toString(modelStream, StandardCharsets.UTF_8);
      Model model = new Model();
      model.loadModelFromText(modelString);
      FileAdapter fileAdapter = new FileAdapter(policyInputStream);
      enforcer = new Enforcer(model, fileAdapter);
    }
  }

  /**
   * "role1" is the OWNER of metalake1. Determine whether role1 has the OWNER permission for
   * metalake1.
   */
  @Test
  public void testMetalakeOwner() {
    Assertions.assertTrue(
        enforcer.enforce("role1", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce(
            "role1",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce("role1", MetadataObject.Type.METALAKE.name(), "metalake2", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce(
            "role1",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
  }

  /**
   * "role2" is the OWNER of catalog1. Determine whether role2 has the OWNER permission for
   * catalog2.
   */
  @Test
  public void testCatalogOwner() {
    Assertions.assertTrue(
        enforcer.enforce("role2", MetadataObject.Type.CATALOG.name(), "catalog1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce(
            "role2",
            MetadataObject.Type.CATALOG.name(),
            "catalog1",
            Privilege.Name.USE_SCHEMA.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role2",
            MetadataObject.Type.CATALOG.name(),
            "catalog1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce("role2", MetadataObject.Type.CATALOG.name(), "catalog2", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce(
            "role2",
            MetadataObject.Type.CATALOG.name(),
            "catalog2",
            Privilege.Name.USE_CATALOG.name()));
  }

  /**
   * role3 is the owner of schema1. Determine whether role3 has the owner permission for schema1.
   */
  @Test
  public void testSchemaOwner() {
    Assertions.assertTrue(
        enforcer.enforce("role3", MetadataObject.Type.SCHEMA.name(), "schema1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce(
            "role3",
            MetadataObject.Type.SCHEMA.name(),
            "schema1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce("role3", MetadataObject.Type.SCHEMA.name(), "schema2", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce(
            "role3",
            MetadataObject.Type.SCHEMA.name(),
            "schema2",
            Privilege.Name.SELECT_TABLE.name()));
  }

  /** "role4" is the owner of table1. Determine whether role4 has the permissions for table1. */
  @Test
  public void testTableOwner() {
    Assertions.assertTrue(
        enforcer.enforce("role4", MetadataObject.Type.TABLE.name(), "table1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce(
            "role4",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role4",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce("role3", MetadataObject.Type.SCHEMA.name(), "table1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role3", MetadataObject.Type.SCHEMA.name(), "table2", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce(
            "role4",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.SELECT_TABLE.name()));
  }

  /** Check whether a role can have privileges. */
  @Test
  public void testRolePrivilege() {
    // "role5" has partial privilege.
    Assertions.assertFalse(
        enforcer.enforce("role5", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce(
            "role5",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertTrue(
        enforcer.enforce(
            "role5",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertTrue(
        enforcer.enforce(
            "role5",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role5",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role5",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role5",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role5",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));

    // role1000 has no privilege.
    Assertions.assertFalse(
        enforcer.enforce("role1000", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));

    Assertions.assertFalse(
        enforcer.enforce(
            "role1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role1000",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role1000",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "role1000",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));
  }

  /**
   * Determine whether a group, after being assigned a role, can inherit the permissions of that
   * role.
   */
  @Test
  public void testGroupPrivilege() {
    // "group1" possesses role5, and therefore, group5 has the permissions of role5.
    Assertions.assertFalse(
        enforcer.enforce("group1", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce(
            "group1",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertTrue(
        enforcer.enforce(
            "group1",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertTrue(
        enforcer.enforce(
            "group1",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));

    // group1000 has no roles and therefore has no permissions.
    Assertions.assertFalse(
        enforcer.enforce("group1000", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));

    Assertions.assertFalse(
        enforcer.enforce(
            "group1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1000",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1000",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "group1000",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));
  }

  /**
   * Determine whether a user can have the corresponding permissions after being granted a role or
   * belonging to a user group.
   */
  @Test
  public void testUserPrivilege() {
    // ”user1" has the privilege of role5.
    Assertions.assertFalse(
        enforcer.enforce("user1", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce(
            "user1",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertTrue(
        enforcer.enforce(
            "user1",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertTrue(
        enforcer.enforce(
            "user1",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));

    // ”user2" has the privilege of group1.
    Assertions.assertFalse(
        enforcer.enforce("user2", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce(
            "user2",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertTrue(
        enforcer.enforce(
            "user2",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user2",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user2",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user2",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user2",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user2",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));

    // "user3" has the privilege of both role1 and group1.
    Assertions.assertTrue(
        enforcer.enforce("user3", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce(
            "user3",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertTrue(
        enforcer.enforce(
            "user3",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user3",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user3",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user3",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user3",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user3",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));

    Assertions.assertFalse(
        enforcer.enforce("user1000", MetadataObject.Type.METALAKE.name(), "metalake1", "OWNER"));

    // "user1000" has no roles and therefore has no permissions.
    Assertions.assertFalse(
        enforcer.enforce(
            "user1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake1",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1000",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.MODIFY_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.SELECT_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1000",
            MetadataObject.Type.METALAKE.name(),
            "metalake2",
            Privilege.Name.USE_CATALOG.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1000",
            MetadataObject.Type.TABLE.name(),
            "table2",
            Privilege.Name.MODIFY_TABLE.name()));
    Assertions.assertFalse(
        enforcer.enforce(
            "user1000",
            MetadataObject.Type.TABLE.name(),
            "table1",
            Privilege.Name.SELECT_TABLE.name()));
  }
}
