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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.model.Model;
import org.casbin.jcasbin.persist.file_adapter.FileAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Test for jcasbin model. */
public class TestJcasbinModel {

  private static Enforcer enforcer;

  /** mock jcasbin policy */
  private static final String JCASBIN_POLICY =
      "p,role1,metalake1,METALAKE,metalake1,OWNER,allow\n"
          + "\n"
          + "p,role2,metalake1,CATALOG,catalog1,OWNER,allow\n"
          + "\n"
          + "p,role3,metalake1,SCHEMA,schema1,OWNER,allow\n"
          + "\n"
          + "p,role4,metalake1,TABLE,table1,OWNER,allow\n"
          + "\n"
          + "p,role5,metalake1,METALAKE,metalake1,SELECT_TABLE,allow\n"
          + "p,role5,metalake1,METALAKE,metalake1,USE_CATALOG,allow\n"
          + "p,role5,metalake1,TABLE,table1,SELECT_TABLE,allow\n"
          + "\n"
          + "g,user1,role5\n"
          + "\n"
          + "g,group1,role5\n"
          + "\n"
          + "g,user2,group1\n"
          + "\n"
          + "g,user3,role1\n"
          + "g,user3,group1";

  @BeforeAll
  public static void init() throws IOException {
    try (InputStream modelStream =
            TestJcasbinModel.class.getResourceAsStream("/jcasbin_model.conf");
        InputStream policyInputStream =
            new ByteArrayInputStream(JCASBIN_POLICY.getBytes(StandardCharsets.UTF_8))) {
      Assertions.assertNotNull(modelStream);
      String string = IOUtils.toString(modelStream, StandardCharsets.UTF_8);
      Model model = new Model();
      model.loadModelFromText(string);
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
    Assertions.assertTrue(enforcer.enforce("role1", "metalake1", "METALAKE", "metalake1", "OWNER"));
    Assertions.assertTrue(
        enforcer.enforce("role1", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("role1", "metalake2", "METALAKE", "metalake1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role1", "metalake2", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("role1", "metalake1", "METALAKE", "metalake2", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role1", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
  }

  /**
   * "role2" is the OWNER of catalog1. Determine whether role2 has the OWNER permission for
   * catalog2.
   */
  @Test
  public void testCatalogOwner() {
    Assertions.assertTrue(enforcer.enforce("role2", "metalake1", "CATALOG", "catalog1", "OWNER"));
    Assertions.assertTrue(
        enforcer.enforce("role2", "metalake1", "CATALOG", "catalog1", "USE_SCHEMA"));
    Assertions.assertTrue(
        enforcer.enforce("role2", "metalake1", "CATALOG", "catalog1", "USE_CATALOG"));
    Assertions.assertFalse(enforcer.enforce("role2", "metalake2", "CATALOG", "catalog1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role2", "metalake2", "CATALOG", "catalog1", "USE_CATALOG"));
    Assertions.assertFalse(enforcer.enforce("role2", "metalake1", "CATALOG", "catalog2", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role2", "metalake1", "CATALOG", "catalog2", "USE_CATALOG"));
  }

  /**
   * role3 is the owner of schema1. Determine whether role3 has the owner permission for schema1.
   */
  @Test
  public void testSchemaOwner() {
    Assertions.assertTrue(enforcer.enforce("role3", "metalake1", "SCHEMA", "schema1", "OWNER"));
    Assertions.assertTrue(
        enforcer.enforce("role3", "metalake1", "SCHEMA", "schema1", "SELECT_TABLE"));
    Assertions.assertFalse(enforcer.enforce("role3", "metalake2", "SCHEMA", "schema1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role3", "metalake2", "SCHEMA", "schema1", "SELECT_TABLE"));
    Assertions.assertFalse(enforcer.enforce("role3", "metalake1", "SCHEMA", "schema2", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role3", "metalake1", "SCHEMA", "schema2", "SELECT_TABLE"));
  }

  /** "role4" is the owner of table1. Determine whether role4 has the permissions for table1. */
  @Test
  public void testTableOwner() {
    Assertions.assertTrue(enforcer.enforce("role4", "metalake1", "TABLE", "table1", "OWNER"));
    Assertions.assertTrue(
        enforcer.enforce("role4", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));
    Assertions.assertTrue(
        enforcer.enforce("role4", "metalake1", "TABLE", "table1", "SELECT_TABLE"));
    Assertions.assertFalse(enforcer.enforce("role3", "metalake2", "SCHEMA", "table1", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role4", "metalake2", "TABLE", "table1", "SELECT_TABLE"));
    Assertions.assertFalse(enforcer.enforce("role3", "metalake1", "SCHEMA", "table2", "OWNER"));
    Assertions.assertFalse(
        enforcer.enforce("role4", "metalake1", "TABLE", "table2", "SELECT_TABLE"));
  }

  /** Check whether a role can have privileges. */
  @Test
  public void testRolePrivilege() {
    // "role5" has partial privilege.
    Assertions.assertFalse(
        enforcer.enforce("role5", "metalake1", "METALAKE", "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce("role5", "metalake1", "METALAKE", "metalake1", "SELECT_TABLE"));
    Assertions.assertTrue(
        enforcer.enforce("role5", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertTrue(
        enforcer.enforce("role5", "metalake1", "TABLE", "table1", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("role5", "metalake1", "METALAKE", "metalake2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("role5", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("role5", "metalake1", "TABLE", "table2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("role5", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));

    // role1000 has no privilege.
    Assertions.assertFalse(
        enforcer.enforce("role1000", "metalake1", "METALAKE", "metalake1", "OWNER"));

    Assertions.assertFalse(
        enforcer.enforce("role1000", "metalake1", "METALAKE", "metalake1", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("role1000", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("role1000", "metalake1", "TABLE", "table1", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("role1000", "metalake1", "METALAKE", "metalake2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("role1000", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("role1000", "metalake1", "TABLE", "table2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("role1000", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));
  }

  /**
   * Determine whether a group, after being assigned a role, can inherit the permissions of that
   * role.
   */
  @Test
  public void testGroupPrivilege() {
    // "group1" possesses role5, and therefore, group5 has the permissions of role5.
    Assertions.assertFalse(
        enforcer.enforce("group1", "metalake1", "METALAKE", "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce("group1", "metalake1", "METALAKE", "metalake1", "SELECT_TABLE"));
    Assertions.assertTrue(
        enforcer.enforce("group1", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertTrue(
        enforcer.enforce("group1", "metalake1", "TABLE", "table1", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("group1", "metalake1", "METALAKE", "metalake2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("group1", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("group1", "metalake1", "TABLE", "table2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("group1", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));

    // group1000 has no roles and therefore has no permissions.
    Assertions.assertFalse(
        enforcer.enforce("group1000", "metalake1", "METALAKE", "metalake1", "OWNER"));

    Assertions.assertFalse(
        enforcer.enforce("group1000", "metalake1", "METALAKE", "metalake1", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("group1000", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("group1000", "metalake1", "TABLE", "table1", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("group1000", "metalake1", "METALAKE", "metalake2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("group1000", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("group1000", "metalake1", "TABLE", "table2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("group1000", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));
  }

  /**
   * Determine whether a user can have the corresponding permissions after being granted a role or
   * belonging to a user group.
   */
  @Test
  public void testUserPrivilege() {
    // ”user1" has the privilege of role5.
    Assertions.assertFalse(
        enforcer.enforce("user1", "metalake1", "METALAKE", "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce("user1", "metalake1", "METALAKE", "metalake1", "SELECT_TABLE"));
    Assertions.assertTrue(
        enforcer.enforce("user1", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertTrue(
        enforcer.enforce("user1", "metalake1", "TABLE", "table1", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user1", "metalake1", "METALAKE", "metalake2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user1", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("user1", "metalake1", "TABLE", "table2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user1", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));

    // ”user2" has the privilege of group1.
    Assertions.assertFalse(
        enforcer.enforce("user2", "metalake1", "METALAKE", "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce("user2", "metalake1", "METALAKE", "metalake1", "SELECT_TABLE"));
    Assertions.assertTrue(
        enforcer.enforce("user2", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("user2", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user2", "metalake1", "METALAKE", "metalake2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user2", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("user2", "metalake1", "TABLE", "table2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user2", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));

    // "user3" has the privilege of both role1 and group1.
    Assertions.assertTrue(enforcer.enforce("user3", "metalake1", "METALAKE", "metalake1", "OWNER"));

    Assertions.assertTrue(
        enforcer.enforce("user3", "metalake1", "METALAKE", "metalake1", "SELECT_TABLE"));
    Assertions.assertTrue(
        enforcer.enforce("user3", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("user3", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user3", "metalake1", "METALAKE", "metalake2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user3", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("user3", "metalake1", "TABLE", "table2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user3", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));

    Assertions.assertFalse(
        enforcer.enforce("user1000", "metalake1", "METALAKE", "metalake1", "OWNER"));

    // "user1000" has no roles and therefore has no permissions.
    Assertions.assertFalse(
        enforcer.enforce("user1000", "metalake1", "METALAKE", "metalake1", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user1000", "metalake1", "METALAKE", "metalake1", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("user1000", "metalake1", "TABLE", "table1", "MODIFY_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user1000", "metalake1", "METALAKE", "metalake2", "SELECT_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user1000", "metalake1", "METALAKE", "metalake2", "USE_CATALOG"));
    Assertions.assertFalse(
        enforcer.enforce("user1000", "metalake1", "TABLE", "table2", "MODIFY_TABLE"));
    Assertions.assertFalse(
        enforcer.enforce("user1000", "metalake1", "TABLE", "table1", "SELECT_TABLE"));
  }
}
