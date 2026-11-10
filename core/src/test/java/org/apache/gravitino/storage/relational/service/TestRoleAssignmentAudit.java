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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;

class TestRoleAssignmentAudit extends TestJDBCBackend {

  @TestTemplate
  void testGrantRevokeAndRegrantPreserveIndependentAssignmentAudits()
      throws IOException, SQLException {
    AuditInfo created =
        AuditInfo.builder()
            .withCreator("creator")
            .withCreateTime(Instant.parse("2026-01-01T00:00:00Z"))
            .build();
    for (boolean group : new boolean[] {false, true}) {
      String metalake = group ? "group_assignments" : "user_assignments";
      createAndInsertMakeLake(metalake);
      createAndInsertCatalog(metalake, "catalog");
      RoleEntity first =
          createRoleEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              AuthorizationUtils.ofRoleNamespace(metalake),
              "first",
              created,
              "catalog");
      RoleEntity second =
          createRoleEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              AuthorizationUtils.ofRoleNamespace(metalake),
              "second",
              created,
              "catalog");
      backend.insert(first, false);
      backend.insert(second, false);
      long principalId = RandomIdGenerator.INSTANCE.nextId();
      Namespace namespace =
          group
              ? AuthorizationUtils.ofGroupNamespace(metalake)
              : AuthorizationUtils.ofUserNamespace(metalake);
      if (group) {
        backend.insert(
            createGroupEntity(principalId, namespace, "principal", created, List.of(), List.of()),
            false);
      } else {
        backend.insert(createUserEntity(principalId, namespace, "principal", created), false);
      }

      AuditInfo firstGrant = grantAudit(created, "first_granter", 1);
      updateAssignments(group, principalId, namespace, firstGrant, List.of(first));
      Map<Long, AuditInfo> firstAssignments = assignmentAudits(group, principalId);
      assertEquals("first_granter", firstAssignments.get(first.id()).creator());
      assertEquals(firstGrant.lastModifiedTime(), firstAssignments.get(first.id()).createTime());

      AuditInfo secondGrant = grantAudit(created, "second_granter", 2);
      updateAssignments(group, principalId, namespace, secondGrant, List.of(first, second));
      Map<Long, AuditInfo> both = assignmentAudits(group, principalId);
      assertEquals(firstAssignments.get(first.id()), both.get(first.id()));
      assertEquals("second_granter", both.get(second.id()).creator());
      assertEquals(secondGrant.lastModifiedTime(), both.get(second.id()).createTime());

      updateAssignments(
          group,
          principalId,
          namespace,
          grantAudit(created, "repeat_granter", 3),
          List.of(first, second));
      assertEquals(both, assignmentAudits(group, principalId));
      updateAssignments(
          group, principalId, namespace, grantAudit(created, "revoker", 4), List.of(second));
      assertEquals(
          Map.of(second.id(), both.get(second.id())), assignmentAudits(group, principalId));

      AuditInfo regrant = grantAudit(created, "new_granter", 5);
      updateAssignments(group, principalId, namespace, regrant, List.of(first, second));
      Map<Long, AuditInfo> regranted = assignmentAudits(group, principalId);
      assertEquals("new_granter", regranted.get(first.id()).creator());
      assertEquals(regrant.lastModifiedTime(), regranted.get(first.id()).createTime());
      assertEquals(both.get(second.id()), regranted.get(second.id()));
      AuditInfo principalAudit =
          group
              ? GroupMetaService.getInstance()
                  .getGroupByIdentifier(AuthorizationUtils.ofGroup(metalake, "principal"))
                  .auditInfo()
              : UserMetaService.getInstance()
                  .getUserByIdentifier(AuthorizationUtils.ofUser(metalake, "principal"))
                  .auditInfo();
      assertEquals(created.creator(), principalAudit.creator());
      assertEquals(created.createTime(), principalAudit.createTime());
    }
  }

  private AuditInfo grantAudit(AuditInfo created, String actor, int seconds) {
    return AuditInfo.builder()
        .withCreator(created.creator())
        .withCreateTime(created.createTime())
        .withLastModifier(actor)
        .withLastModifiedTime(created.createTime().plusSeconds(seconds))
        .build();
  }

  private void updateAssignments(
      boolean group, long id, Namespace namespace, AuditInfo audit, List<RoleEntity> roles)
      throws IOException {
    List<String> names = roles.stream().map(RoleEntity::name).toList();
    List<Long> ids = roles.stream().map(RoleEntity::id).toList();
    if (group) {
      GroupEntity principal = createGroupEntity(id, namespace, "principal", audit, names, ids);
      GroupMetaService.getInstance().updateGroup(principal.nameIdentifier(), ignored -> principal);
    } else {
      UserEntity principal = createUserEntity(id, namespace, "principal", audit, names, ids);
      UserMetaService.getInstance().updateUser(principal.nameIdentifier(), ignored -> principal);
    }
  }

  private Map<Long, AuditInfo> assignmentAudits(boolean group, long id)
      throws SQLException, IOException {
    String kind = group ? "group" : "user";
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT role_id, audit_info FROM "
                    + kind
                    + "_role_rel WHERE "
                    + kind
                    + "_id = ? AND deleted_at = 0")) {
      statement.setLong(1, id);
      try (ResultSet rows = statement.executeQuery()) {
        Map<Long, AuditInfo> audits = new HashMap<>();
        while (rows.next()) {
          audits.put(
              rows.getLong("role_id"),
              JsonUtils.anyFieldMapper().readValue(rows.getString("audit_info"), AuditInfo.class));
        }
        return audits;
      }
    }
  }
}
