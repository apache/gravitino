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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.service.EntityIdService;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Counts real database queries in list loading and filtering, independently of list size. */
class TestPrincipalListQueryCount {
  private static final Logger LOG = LoggerFactory.getLogger(TestPrincipalListQueryCount.class);

  @TempDir Path tempDir;

  @Test
  void testManagementListsHaveBoundedQueries() throws Exception {
    Config config = new Config(false) {};
    config.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        "jdbc:h2:file:" + tempDir.resolve("metadata") + ";MODE=MYSQL;AUTO_SERVER=FALSE");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "test");
    config.set(Configs.ENABLE_AUTHORIZATION, true);
    EntityIdResolver previousResolver =
        (EntityIdResolver)
            FieldUtils.readStaticField(EntityIdService.class, "entityIdResolver", true);
    Object previousExecutor =
        FieldUtils.readStaticField(MetadataAuthzHelper.class, "executor", true);
    List<Executable> queryCountAssertions = new ArrayList<>();
    try (MockedStatic<GravitinoEnv> envStatic = mockStatic(GravitinoEnv.class);
        MockedStatic<GravitinoAuthorizerProvider> providerStatic =
            mockStatic(GravitinoAuthorizerProvider.class);
        JDBCBackend backend = new JDBCBackend()) {
      GravitinoEnv env = mock(GravitinoEnv.class);
      envStatic.when(GravitinoEnv::getInstance).thenReturn(env);
      when(env.config()).thenReturn(config);
      when(env.cacheEnabled()).thenReturn(true);
      EntityStore store = mock(EntityStore.class);
      SupportsRelationOperations relations = mock(SupportsRelationOperations.class);
      when(env.entityStore()).thenReturn(store);
      when(store.relationOperations()).thenReturn(relations);
      when(relations.batchListEntitiesByRelation(
              eq(SupportsRelationOperations.Type.OWNER_REL), anyList(), any()))
          .thenAnswer(
              call ->
                  backend.batchListEntitiesByRelation(
                      call.getArgument(0), call.getArgument(1), call.getArgument(2)));
      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      providerStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
      when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
      // Isolate the storage/list-filter path: the caller has a metalake management grant.
      when(authorizer.authorize(any(), any(), any(), any(), any()))
          .thenAnswer(
              call -> {
                MetadataObject object = call.getArgument(2);
                Privilege.Name privilege = call.getArgument(3);
                return object.type() == MetadataObject.Type.METALAKE
                    && (privilege == Privilege.Name.MANAGE_USERS
                        || privilege == Privilege.Name.MANAGE_GROUPS
                        || privilege == Privilege.Name.MANAGE_GRANTS);
              });
      FieldUtils.writeStaticField(
          MetadataAuthzHelper.class, "executor", (Executor) Runnable::run, true);
      backend.initialize(config);
      EntityIdService.initialize(new RelationalEntityStoreIdResolver());
      try (Connection connection =
          DriverManager.getConnection(
              config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL), "root", "test")) {
        for (int size : new int[] {1, 1003, 10000}) {
          String metalake = "scale" + size;
          insertPrincipals(connection, size, metalake);
          for (Entity.EntityType type :
              List.of(Entity.EntityType.USER, Entity.EntityType.GROUP, Entity.EntityType.ROLE)) {
            for (boolean details : new boolean[] {false, true}) {
              try (Statement statement = connection.createStatement()) {
                statement.execute("SET QUERY_STATISTICS FALSE");
                statement.execute("SET QUERY_STATISTICS TRUE");
              }
              long start = System.nanoTime();
              Namespace namespace =
                  switch (type) {
                    case USER -> AuthorizationUtils.ofUserNamespace(metalake);
                    case GROUP -> AuthorizationUtils.ofGroupNamespace(metalake);
                    default -> AuthorizationUtils.ofRoleNamespace(metalake);
                  };
              List<? extends HasIdentifier> entities = backend.list(namespace, type, details);
              String expression =
                  switch (type) {
                    case USER -> AuthorizationExpressionConstants
                        .LOAD_USER_AUTHORIZATION_EXPRESSION;
                    case GROUP -> AuthorizationExpressionConstants
                        .LOAD_GROUP_AUTHORIZATION_EXPRESSION;
                    default -> AuthorizationExpressionConstants.LOAD_ROLE_AUTHORIZATION_EXPRESSION;
                  };
              int returned =
                  PrincipalUtils.doAs(
                      new UserPrincipal("manager"),
                      () -> {
                        if (details) {
                          return MetadataAuthzHelper.filterByExpression(
                                  metalake,
                                  expression,
                                  type,
                                  entities.toArray(new HasIdentifier[0]),
                                  HasIdentifier::nameIdentifier)
                              .length;
                        }
                        NameIdentifier[] identifiers =
                            entities.stream()
                                .map(HasIdentifier::nameIdentifier)
                                .toArray(NameIdentifier[]::new);
                        return MetadataAuthzHelper.filterByExpression(
                                metalake, expression, type, identifiers)
                            .length;
                      });
              long millis = (System.nanoTime() - start) / 1_000_000;
              long selects = countSelects(connection);
              LOG.info(
                  "Principal list: type={}, size={}, details={}, SELECTs={}, elapsedMs={}",
                  type,
                  size,
                  details,
                  selects,
                  millis);
              Assertions.assertEquals(size, returned);
              queryCountAssertions.add(
                  () ->
                      Assertions.assertEquals(
                          type == Entity.EntityType.ROLE || !details ? 1 : 2,
                          selects,
                          type + " size=" + size + " details=" + details));
            }
          }
        }
      }
      Assertions.assertAll(queryCountAssertions);
    } finally {
      EntityIdService.initialize(previousResolver);
      FieldUtils.writeStaticField(MetadataAuthzHelper.class, "executor", previousExecutor, true);
    }
  }

  private static long countSelects(Connection connection) throws Exception {
    long count = 0;
    try (Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT SQL_STATEMENT, EXECUTION_COUNT FROM INFORMATION_SCHEMA.QUERY_STATISTICS")) {
      while (rows.next()) {
        String sql = rows.getString(1).trim().toUpperCase(Locale.ROOT);
        if (sql.startsWith("SELECT") && !sql.contains("INFORMATION_SCHEMA")) {
          count += rows.getLong(2);
        }
      }
    }
    return count;
  }

  private static void insertPrincipals(Connection connection, int size, String metalake)
      throws Exception {
    String audit =
        JsonUtils.anyFieldMapper()
            .writeValueAsString(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build());
    try (PreparedStatement insert =
        connection.prepareStatement(
            "INSERT INTO metalake_meta (metalake_id, metalake_name, audit_info, schema_version) VALUES (?, ?, ?, '{}')")) {
      insert.setLong(1, size);
      insert.setString(2, metalake);
      insert.setString(3, audit);
      insert.executeUpdate();
    }
    for (String kind : List.of("user", "group", "role")) {
      try (PreparedStatement insert =
          connection.prepareStatement(
              "INSERT INTO "
                  + kind
                  + "_meta ("
                  + kind
                  + "_id, "
                  + kind
                  + "_name, metalake_id, audit_info) VALUES (?, ?, ?, ?)")) {
        for (int i = 0; i < size; i++) {
          insert.setLong(1, size * 100000L + i);
          insert.setString(2, kind + i);
          insert.setLong(3, size);
          insert.setString(4, audit);
          insert.addBatch();
        }
        insert.executeBatch();
      }
      if (kind.equals("role")) {
        try (Statement statement = connection.createStatement()) {
          statement.executeUpdate(
              "UPDATE role_meta SET properties = '{}' WHERE properties IS NULL");
        }
      }
    }
  }
}
