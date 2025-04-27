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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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

  @BeforeAll
  public static void init() throws IOException {
    InputStream modelStream = TestJcasbinModel.class.getResourceAsStream("/jcasbin_model.conf");
    Assertions.assertNotNull(modelStream);
    String string = IOUtils.toString(modelStream, StandardCharsets.UTF_8);
    Model model = new Model();
    model.loadModelFromText(string);
    URL resource = TestJcasbinModel.class.getResource("/jcasbin_policy.txt");
    Assertions.assertNotNull(resource);
    FileAdapter fileAdapter = new FileAdapter(resource.getPath());
    enforcer = new Enforcer(model, fileAdapter);
  }

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

  @Test
  public void testRolePrivilege() {
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

  @Test
  public void testGroupPrivilege() {
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

  @Test
  public void testUserPrivilege() {
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
