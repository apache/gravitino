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
package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.OAuth2TokenProvider;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.ChangedOwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration tests for JCasbin authorization cache consistency. */
public class JcasbinAuthorizationCacheConsistencyIT extends BaseIT {

  private static final String METALAKE = "jcasbin_cache_it";
  private static final String ADMIN = "cache_admin";
  private static final String ROLE_USER = "cache_role_user";
  private static final String USER_ROLE_USER = "cache_user_role_user";
  private static final String GROUP_USER = "cache_group_user";
  private static final String OWNER_A = "cache_owner_a";
  private static final String OWNER_B = "cache_owner_b";
  private static final String DELETED_OWNER = "cache_deleted_owner";
  private static final String RENAME_USER = "cache_rename_user";
  private static final String GROUP = "cache_group";
  private static final String CATALOG = "cache_catalog";
  private static final String RENAME_CATALOG = "cache_rename_catalog";
  private static final String RENAMED_CATALOG = "cache_renamed_catalog";

  private static final KeyPair KEY_PAIR = Keys.keyPairFor(SignatureAlgorithm.RS256);
  private static final String PUBLIC_KEY =
      new String(
          Base64.getEncoder().encode(KEY_PAIR.getPublic().getEncoded()), StandardCharsets.UTF_8);

  private static GravitinoAdminClient roleUserClient;
  private static GravitinoAdminClient userRoleUserClient;
  private static GravitinoAdminClient groupUserClient;
  private static GravitinoAdminClient ownerAClient;
  private static GravitinoAdminClient ownerBClient;
  private static GravitinoAdminClient deletedOwnerClient;
  private static GravitinoAdminClient renameUserClient;

  @SuppressWarnings("JavaUtilDate")
  private static String token(String user, List<String> groups) {
    return Jwts.builder()
        .setSubject(user)
        .claim("groups", groups)
        .setExpiration(new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(30)))
        .setAudience("service1")
        .signWith(KEY_PAIR.getPrivate(), SignatureAlgorithm.RS256)
        .compact();
  }

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    OAuthMockDataProvider.getInstance()
        .setTokenData(token(ADMIN, Collections.emptyList()).getBytes(StandardCharsets.UTF_8));
    customConfigs.putAll(
        ImmutableMap.<String, String>builder()
            .put(Configs.ENABLE_AUTHORIZATION.getKey(), "true")
            .put(Configs.CACHE_ENABLED.getKey(), "false")
            .put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.OAUTH.name().toLowerCase())
            .put(Configs.SERVICE_ADMINS.getKey(), ADMIN)
            .put(Configs.GRAVITINO_AUTHORIZATION_CACHE_EXPIRATION_SECS.getKey(), "3600")
            .put(Configs.GRAVITINO_AUTHORIZATION_CHANGE_POLL_INTERVAL_SECS.getKey(), "1")
            .put(OAuthConfig.SERVICE_AUDIENCE.getKey(), "service1")
            .put(OAuthConfig.DEFAULT_SIGN_KEY.getKey(), PUBLIC_KEY)
            .put(OAuthConfig.ALLOW_SKEW_SECONDS.getKey(), "6")
            .put(OAuthConfig.DEFAULT_SERVER_URI.getKey(), "test")
            .put(OAuthConfig.DEFAULT_TOKEN_PATH.getKey(), "test")
            .build());
    super.startIntegrationTest();

    roleUserClient = oauthClient(ROLE_USER);
    userRoleUserClient = oauthClient(USER_ROLE_USER);
    groupUserClient = oauthClient(GROUP_USER, GROUP);
    ownerAClient = oauthClient(OWNER_A);
    ownerBClient = oauthClient(OWNER_B);
    deletedOwnerClient = oauthClient(DELETED_OWNER);
    renameUserClient = oauthClient(RENAME_USER);

    GravitinoMetalake metalake = client.createMetalake(METALAKE, "comment", Collections.emptyMap());
    for (String user :
        ImmutableList.of(
            ROLE_USER, USER_ROLE_USER, GROUP_USER, OWNER_A, OWNER_B, DELETED_OWNER, RENAME_USER)) {
      metalake.addUser(user);
    }
    metalake.addGroup(GROUP);
    metalake.createCatalog(
        CATALOG, Catalog.Type.FILESET, "hadoop", "comment", Collections.emptyMap());
    metalake.createCatalog(
        RENAME_CATALOG, Catalog.Type.FILESET, "hadoop", "comment", Collections.emptyMap());
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    closeClient(roleUserClient);
    closeClient(userRoleUserClient);
    closeClient(groupUserClient);
    closeClient(ownerAClient);
    closeClient(ownerBClient);
    closeClient(deletedOwnerClient);
    closeClient(renameUserClient);
    client.dropMetalake(METALAKE, true);
    super.stopIntegrationTest();
  }

  @Test
  void testRoleUserAndGroupChangesAreVisibleImmediately() {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);

    String rolePrivilegeRole = "cache_role_privilege_role";
    metalake.createRole(rolePrivilegeRole, Collections.emptyMap(), Collections.emptyList());
    metalake.grantRolesToUser(ImmutableList.of(rolePrivilegeRole), ROLE_USER);
    assertLoadCatalogForbidden(roleUserClient, CATALOG);
    metalake.grantPrivilegesToRole(
        rolePrivilegeRole,
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        ImmutableSet.of(Privileges.UseCatalog.allow()));
    assertLoadCatalogAllowed(roleUserClient, CATALOG);
    metalake.revokePrivilegesFromRole(
        rolePrivilegeRole,
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        ImmutableSet.of(Privileges.UseCatalog.allow()));
    assertLoadCatalogForbidden(roleUserClient, CATALOG);

    String userRole = "cache_user_role";
    metalake.createRole(
        userRole,
        Collections.emptyMap(),
        ImmutableList.of(
            SecurableObjects.ofCatalog(CATALOG, ImmutableList.of(Privileges.UseCatalog.allow()))));
    assertLoadCatalogForbidden(userRoleUserClient, CATALOG);
    metalake.grantRolesToUser(ImmutableList.of(userRole), USER_ROLE_USER);
    assertLoadCatalogAllowed(userRoleUserClient, CATALOG);
    metalake.revokeRolesFromUser(ImmutableList.of(userRole), USER_ROLE_USER);
    assertLoadCatalogForbidden(userRoleUserClient, CATALOG);

    String groupRole = "cache_group_role";
    metalake.createRole(
        groupRole,
        Collections.emptyMap(),
        ImmutableList.of(
            SecurableObjects.ofCatalog(CATALOG, ImmutableList.of(Privileges.UseCatalog.allow()))));
    assertLoadCatalogForbidden(groupUserClient, CATALOG);
    metalake.grantRolesToGroup(ImmutableList.of(groupRole), GROUP);
    assertLoadCatalogAllowed(groupUserClient, CATALOG);
    metalake.revokeRolesFromGroup(ImmutableList.of(groupRole), GROUP);
    assertLoadCatalogForbidden(groupUserClient, CATALOG);
  }

  @Test
  void testOwnerChangesAndOwnerDeletesAreVisibleWithinPollInterval() {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    MetadataObject catalogObject =
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG);

    OwnerCursor ownerCursor = ownerCursor();
    metalake.setOwner(catalogObject, OWNER_A, Owner.Type.USER);
    List<ChangedOwnerInfo> ownerChanges = changedOwners(ownerCursor);
    Assertions.assertTrue(
        ownerChanges.stream()
            .anyMatch(
                change ->
                    MetadataObject.Type.CATALOG.name().equals(change.getMetadataObjectType())));
    assertLoadCatalogAllowed(ownerAClient, CATALOG);
    assertLoadCatalogForbidden(ownerBClient, CATALOG);

    ownerCursor = ownerCursor();
    metalake.setOwner(catalogObject, OWNER_B, Owner.Type.USER);
    ownerChanges = changedOwners(ownerCursor);
    Assertions.assertTrue(
        ownerChanges.stream()
            .anyMatch(
                change ->
                    MetadataObject.Type.CATALOG.name().equals(change.getMetadataObjectType())));
    assertLoadCatalogForbidden(ownerAClient, CATALOG);
    assertLoadCatalogAllowed(ownerBClient, CATALOG);

    metalake.setOwner(catalogObject, DELETED_OWNER, Owner.Type.USER);
    assertLoadCatalogAllowed(deletedOwnerClient, CATALOG);
    long catalogId =
        MetadataIdConverter.getID(catalogObject, METALAKE)
            .orElseThrow(() -> new IllegalStateException("Catalog id should exist"));
    Assertions.assertTrue(ownerRelCache().getIfPresent(catalogId).isPresent());
    ownerCursor = ownerCursor();
    metalake.removeUser(DELETED_OWNER);
    awaitChangedOwners(ownerCursor, MetadataObject.Type.CATALOG);
    Awaitility.await()
        .atMost(3, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              Assertions.assertFalse(ownerRelCache().getIfPresent(catalogId).isPresent());
              assertLoadCatalogForbidden(deletedOwnerClient, CATALOG);
            });
  }

  @Test
  void testMetadataRenameChangeLogIsVisibleWithinPollInterval() {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    String renameRole = "cache_rename_role";
    metalake.createRole(
        renameRole,
        Collections.emptyMap(),
        ImmutableList.of(
            SecurableObjects.ofCatalog(
                RENAME_CATALOG, ImmutableList.of(Privileges.UseCatalog.allow()))));
    metalake.grantRolesToUser(ImmutableList.of(renameRole), RENAME_USER);
    assertLoadCatalogAllowed(renameUserClient, RENAME_CATALOG);

    long entityCursor = entityCursor();
    metalake.alterCatalog(RENAME_CATALOG, CatalogChange.rename(RENAMED_CATALOG));
    Awaitility.await()
        .atMost(3, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              List<EntityChangeRecord> changes = entityChanges(entityCursor);
              Assertions.assertTrue(
                  changes.stream()
                      .anyMatch(
                          change ->
                              MetadataObject.Type.CATALOG.name().equals(change.getEntityType())
                                  && OperateType.ALTER == change.getOperateType()
                                  && (METALAKE + "." + RENAME_CATALOG)
                                      .equals(change.getFullName())));
            });
    assertLoadCatalogForbidden(renameUserClient, RENAME_CATALOG);
    assertLoadCatalogAllowed(renameUserClient, RENAMED_CATALOG);
  }

  private GravitinoAdminClient oauthClient(String user, String group) {
    return GravitinoAdminClient.builder(serverUri)
        .withOAuth(new StaticTokenProvider(token(user, ImmutableList.of(group))))
        .build();
  }

  private GravitinoAdminClient oauthClient(String user) {
    return GravitinoAdminClient.builder(serverUri)
        .withOAuth(new StaticTokenProvider(token(user, Collections.emptyList())))
        .build();
  }

  private static void assertLoadCatalogAllowed(GravitinoAdminClient client, String catalog) {
    assertDoesNotThrow(() -> client.loadMetalake(METALAKE).loadCatalog(catalog));
  }

  private static void assertLoadCatalogForbidden(GravitinoAdminClient client, String catalog) {
    assertThrows(
        ForbiddenException.class, () -> client.loadMetalake(METALAKE).loadCatalog(catalog));
  }

  private static OwnerCursor ownerCursor() {
    ChangedOwnerInfo maxChangedOwner =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class, OwnerMetaMapper::selectMaxChangedOwner);
    long maxInsertId =
        Optional.ofNullable(
                SessionUtils.getWithoutCommit(
                    OwnerMetaMapper.class, OwnerMetaMapper::selectMaxChangeId))
            .orElse(0L);
    if (maxChangedOwner == null) {
      return new OwnerCursor(0L, 0L, maxInsertId);
    }
    return new OwnerCursor(maxChangedOwner.getUpdatedAt(), maxChangedOwner.getId(), maxInsertId);
  }

  private static List<ChangedOwnerInfo> changedOwners(OwnerCursor cursor) {
    return SessionUtils.getWithoutCommit(
        OwnerMetaMapper.class,
        mapper ->
            mapper.selectChangedOwners(cursor.updatedAt, cursor.updatedAtId, cursor.insertId));
  }

  private static void awaitChangedOwners(OwnerCursor cursor, MetadataObject.Type metadataType) {
    Awaitility.await()
        .atMost(3, TimeUnit.SECONDS)
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () ->
                Assertions.assertTrue(
                    changedOwners(cursor).stream()
                        .anyMatch(
                            change -> metadataType.name().equals(change.getMetadataObjectType()))));
  }

  private static long entityCursor() {
    return Optional.ofNullable(
            SessionUtils.getWithoutCommit(
                EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId))
        .orElse(0L);
  }

  private static List<EntityChangeRecord> entityChanges(long cursor) {
    return SessionUtils.getWithoutCommit(
        EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(cursor, 500));
  }

  @SuppressWarnings("unchecked")
  private static GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache() {
    GravitinoAuthorizer authorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    if (!(authorizer instanceof JcasbinAuthorizer)) {
      throw new IllegalStateException("Expected JcasbinAuthorizer but got " + authorizer);
    }
    try {
      Field field = JcasbinAuthorizer.class.getDeclaredField("ownerRelCache");
      field.setAccessible(true);
      return (GravitinoCache<Long, Optional<OwnerInfo>>) field.get(authorizer);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to access ownerRelCache", e);
    }
  }

  private static void closeClient(GravitinoAdminClient client) throws IOException {
    if (client != null) {
      client.close();
    }
  }

  private static class OwnerCursor {
    private final long updatedAt;
    private final long updatedAtId;
    private final long insertId;

    private OwnerCursor(long updatedAt, long updatedAtId, long insertId) {
      this.updatedAt = updatedAt;
      this.updatedAtId = updatedAtId;
      this.insertId = insertId;
    }
  }

  private static class StaticTokenProvider extends OAuth2TokenProvider {
    private final String token;

    private StaticTokenProvider(String token) {
      this.token = token;
    }

    @Override
    protected String getAccessToken() {
      return token;
    }
  }
}
