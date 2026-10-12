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
package org.apache.gravitino.server.authorization.jcasbin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.model.Model;
import org.junit.jupiter.api.Test;

/**
 * Compares the per-role policy indexes with the JCasbin model that they replace. JCasbin is a
 * test-only dependency here; the enforcers are populated exactly as the JCasbin authorizer did, and
 * every decision of both implementations must agree.
 *
 * <p>Each scenario loads random roles over a small object space, so that ALLOW and DENY rows on the
 * same key, inactive roles, and roles the caller does not hold are all common. A fixed seed keeps
 * the run reproducible.
 */
public class TestPolicyIndexDifferential {

  private static final long SEED = 20261012L;
  private static final int SCENARIOS = 300;
  private static final int QUERIES_PER_SCENARIO = 150;
  private static final String USER = "user";
  private static final MetadataObject.Type[] TYPES = {
    MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE
  };
  private static final Privilege.Name[] PRIVILEGES = {
    Privilege.Name.USE_CATALOG, Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE
  };
  private static final int OBJECT_IDS = 4;

  @Test
  public void testIndexDecisionsMatchJcasbinModel() throws IOException {
    String modelText;
    try (InputStream in =
        TestPolicyIndexDifferential.class.getResourceAsStream("/jcasbin_reference_model.conf")) {
      modelText = IOUtils.toString(in, StandardCharsets.UTF_8);
    }
    Random random = new Random(SEED);
    int mergedViews = 0;
    for (int scenario = 0; scenario < SCENARIOS; scenario++) {
      if (runScenario(scenario, random, modelText)) {
        mergedViews++;
      }
    }
    // Both the per-role probes and the merged index must be compared.
    assertTrue(mergedViews > SCENARIOS / 4, "too few merged views: " + mergedViews);
    assertTrue(mergedViews < SCENARIOS, "no view kept probing roles");
  }

  /** Runs one scenario and returns whether its view switched to a merged index. */
  private static boolean runScenario(int scenario, Random random, String modelText) {
    Enforcer allowEnforcer = new Enforcer(newModel(modelText));
    Enforcer denyEnforcer = new Enforcer(newModel(modelText));
    Map<Long, CachedRolePolicies> heldRoles = new HashMap<>();
    Map<Long, RoleUpdatedAt> versions = new HashMap<>();
    List<String> heldRoleNames = new ArrayList<>();

    int roleCount = 1 + random.nextInt(6);
    for (long roleId = 1; roleId <= roleCount; roleId++) {
      String roleName = "role" + roleId;
      Map<PolicyKey, Effect> index = new HashMap<>();
      int objects = random.nextInt(5);
      boolean held = random.nextInt(4) != 0;
      for (int i = 0; i < objects; i++) {
        MetadataObject.Type type = TYPES[random.nextInt(TYPES.length)];
        long id = 1L + random.nextInt(OBJECT_IDS);
        List<Privilege> privileges = new ArrayList<>();
        for (Privilege.Name name : PRIVILEGES) {
          int pick = random.nextInt(4);
          if (pick == 1) {
            privileges.add(Privileges.allow(name));
          } else if (pick == 2) {
            privileges.add(Privileges.deny(name));
          }
        }
        // The production compiler, applied to the same objects that feed the JCasbin rows.
        JcasbinAuthorizer.addPolicies(type, id, privileges, index);
        for (Privilege privilege : privileges) {
          addJcasbinRows(allowEnforcer, denyEnforcer, roleId, type, id, privilege);
        }
      }
      // Roles the caller does not hold still load rows into the shared JCasbin enforcers.
      if (held) {
        allowEnforcer.addRoleForUser(USER, String.valueOf(roleId));
        denyEnforcer.addRoleForUser(USER, String.valueOf(roleId));
        heldRoles.put(roleId, new CachedRolePolicies(1L, index));
        versions.put(roleId, new RoleUpdatedAt(roleId, roleName, 1L, 1L));
        heldRoleNames.add(roleName);
      }
    }

    ActiveRoles active = randomActiveRoles(random, heldRoleNames);
    RequestRolePolicies view = new RequestRolePolicies(0L, heldRoles, active, versions);

    for (int query = 0; query < QUERIES_PER_SCENARIO; query++) {
      MetadataObject.Type type = TYPES[random.nextInt(TYPES.length)];
      long id = 1L + random.nextInt(OBJECT_IDS);
      String privilege = PRIVILEGES[random.nextInt(PRIVILEGES.length)].name();
      PolicyKey key = new PolicyKey(type.name(), id, privilege);
      String context = "scenario " + scenario + ", query " + query + ", " + key + ", " + active;

      boolean expectedAllow =
          jcasbinAllow(allowEnforcer, denyEnforcer, active, versions, type, id, privilege);
      assertEquals(expectedAllow, view.evaluate(key, true), "ALLOW mismatch at " + context);
      boolean expectedDeny = denyEnforcer.enforce(USER, type.name(), String.valueOf(id), privilege);
      assertEquals(expectedDeny, view.evaluate(key, false), "DENY mismatch at " + context);
      assertEquals(
          jcasbinHasDeny(denyEnforcer, privilege),
          view.hasDeny(Set.of(privilege)),
          "hasDeny mismatch at " + context);
    }
    return view.isMerged();
  }

  /** The JCasbin authorizer's list shortcut check: any held role carries a matching DENY row. */
  private static boolean jcasbinHasDeny(Enforcer denyEnforcer, String privilege) {
    for (String roleId : denyEnforcer.getRolesForUser(USER)) {
      for (List<String> policy : denyEnforcer.getFilteredNamedPolicy("p", 0, roleId)) {
        if (privilege.equals(policy.get(3))) {
          return true;
        }
      }
    }
    return false;
  }

  /** The JCasbin authorizer's allow check, including its narrowed path for named active roles. */
  private static boolean jcasbinAllow(
      Enforcer allowEnforcer,
      Enforcer denyEnforcer,
      ActiveRoles active,
      Map<Long, RoleUpdatedAt> versions,
      MetadataObject.Type type,
      long id,
      String privilege) {
    String idText = String.valueOf(id);
    if (active.isAll()) {
      return allowEnforcer.enforce(USER, type.name(), idText, privilege);
    }
    if (denyEnforcer.enforce(USER, type.name(), idText, privilege) || active.isNone()) {
      return false;
    }
    for (RoleUpdatedAt version : versions.values()) {
      if (active.roleNames().contains(version.getRoleName())
          && allowEnforcer.enforce(
              String.valueOf(version.getRoleId()), type.name(), idText, privilege)) {
        return true;
      }
    }
    return false;
  }

  /** Writes the rows that the JCasbin authorizer derived from one privilege. */
  private static void addJcasbinRows(
      Enforcer allowEnforcer,
      Enforcer denyEnforcer,
      long roleId,
      MetadataObject.Type type,
      long id,
      Privilege privilege) {
    String role = String.valueOf(roleId);
    String action = privilege.name().name().toUpperCase(Locale.ROOT);
    if (privilege.condition() == Privilege.Condition.DENY) {
      denyEnforcer.addPolicy(role, type.name(), String.valueOf(id), action, "allow");
    }
    allowEnforcer.addPolicy(
        role,
        type.name(),
        String.valueOf(id),
        action,
        privilege.condition().name().toLowerCase(Locale.ROOT));
  }

  private static ActiveRoles randomActiveRoles(Random random, List<String> heldRoleNames) {
    int pick = random.nextInt(4);
    if (pick == 0) {
      return ActiveRoles.all();
    }
    if (pick == 1) {
      return ActiveRoles.none();
    }
    List<String> names = new ArrayList<>();
    for (String name : heldRoleNames) {
      if (random.nextBoolean()) {
        names.add(name);
      }
    }
    // Naming a role the caller does not hold activates nothing.
    names.add("unheldRole");
    return ActiveRoles.of(names);
  }

  private static Model newModel(String modelText) {
    Model model = new Model();
    model.loadModelFromText(modelText);
    return model;
  }
}
