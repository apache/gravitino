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

package org.apache.gravitino.server.authorization.jcasbin;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.main.SyncedEnforcer;
import org.casbin.jcasbin.model.Model;
import org.junit.jupiter.api.Test;

public class TestRequestRoleFunction {

  @Test
  public void testRequestRolesAndDenyPrecedence() throws Exception {
    Enforcer enforcer = newEnforcer();
    enforcer.addPolicy("allow-role", "TABLE", "42", "SELECT_TABLE", "allow");
    enforcer.addPolicy("deny-role", "TABLE", "42", "SELECT_TABLE", "deny");
    assertTrue(enforcer.enforce(Set.of("allow-role"), "TABLE", "42", "SELECT_TABLE"));
    assertFalse(enforcer.enforce(Set.of("allow-role", "deny-role"), "TABLE", "42", "SELECT_TABLE"));
    assertFalse(enforcer.enforce(Set.of("unheld-role"), "TABLE", "42", "SELECT_TABLE"));
    assertFalse(enforcer.enforce(Set.of(), "TABLE", "42", "SELECT_TABLE"));
    assertFalse(enforcer.enforce("allow-role", "TABLE", "42", "SELECT_TABLE"));
    assertFalse(enforcer.enforce(Set.of("allow-role"), "TABLE", "43", "SELECT_TABLE"));
    assertFalse(enforcer.enforce(Set.of("allow-role"), "SCHEMA", "42", "SELECT_TABLE"));
    assertFalse(enforcer.enforce(Set.of("allow-role"), "TABLE", "42", "MODIFY_TABLE"));
  }

  @Test
  public void testDenyEnforcerUsesRoleSet() throws Exception {
    Enforcer deny = newEnforcer();
    // The deny enforcer represents DENY policies with the allow effect to answer existence checks.
    deny.addPolicy("deny-role", "TABLE", "42", "SELECT_TABLE", "allow");
    assertTrue(deny.enforce(Set.of("allow-role", "deny-role"), "TABLE", "42", "SELECT_TABLE"));
    assertFalse(deny.enforce(Set.of("allow-role"), "TABLE", "42", "SELECT_TABLE"));
  }

  private Enforcer newEnforcer() throws Exception {
    Model model = new Model();
    try (InputStream input = getClass().getResourceAsStream("/jcasbin_request_model.conf")) {
      model.loadModelFromText(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    }
    Enforcer enforcer = new SyncedEnforcer(model, new GravitinoAdapter());
    enforcer.addFunction("hasRole", new RequestRoleFunction());
    return enforcer;
  }
}
