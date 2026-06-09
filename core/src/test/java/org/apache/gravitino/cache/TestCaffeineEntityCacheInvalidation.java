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
package org.apache.gravitino.cache;

import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CaffeineEntityCache} invalidation logic, specifically covering the bug
 * where invalidating a role entity did not propagate to the METADATA_OBJECT_ROLE_REL relation cache
 * entries for the role's securable objects.
 *
 * <p>See GitHub issue #11297: {@code listBindingRoleNames} returns stale role after {@code
 * revokePrivilegesFromRole} removes the last privilege.
 */
public class TestCaffeineEntityCacheInvalidation {

  private CaffeineEntityCache cache;
  private AuditInfo auditInfo;

  @BeforeEach
  void setUp() {
    Config config = new Config(false) {};
    cache = new CaffeineEntityCache(config);
    auditInfo = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  }

  /**
   * Builds a RoleEntity that has the given schema as its securable object.
   *
   * @param metalake the metalake name
   * @param roleName the role name
   * @param catalogName the catalog the schema belongs to
   * @param schemaName the schema name used as the role's securable object
   * @return the constructed RoleEntity
   */
  private RoleEntity buildRoleWithSchemaObject(
      String metalake, String roleName, String catalogName, String schemaName) {
    SecurableObject catalogObject = SecurableObjects.ofCatalog(catalogName, Lists.newArrayList());
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schemaName, Lists.newArrayList(Privileges.UseSchema.allow()));
    return RoleEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(roleName)
        .withNamespace(AuthorizationUtils.ofRoleNamespace(metalake))
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(Lists.newArrayList(schemaObject))
        .build();
  }

  /**
   * Core scenario reproducing GitHub issue #11297: after caching the METADATA_OBJECT_ROLE_REL
   * relation for a schema, invalidating the role entity must also remove the stale relation cache.
   *
   * <p>Flow: grant → listBindingRoleNames (caches relation) → revoke (invalidates role) →
   * listBindingRoleNames must miss cache and re-query.
   */
  @Test
  void testRoleInvalidationPropagatesToMetadataObjectRoleRelCache() {
    String metalake = "metalake";
    String catalogName = "catalog";
    String schemaName = "test_schema";
    String roleName = "test_role";

    RoleEntity role = buildRoleWithSchemaObject(metalake, roleName, catalogName, schemaName);
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalogName, schemaName);
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(metalake, roleName);

    // Simulate caching the METADATA_OBJECT_ROLE_REL result (e.g., after listBindingRoleNames)
    cache.put(
        schemaIdent,
        Entity.EntityType.SCHEMA,
        SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
        Lists.newArrayList(role));

    // Verify the relation cache is populated
    Optional<List<RoleEntity>> cached =
        cache.getIfPresent(
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
            schemaIdent,
            Entity.EntityType.SCHEMA);
    Assertions.assertTrue(cached.isPresent(), "Relation cache should be populated after put");
    Assertions.assertEquals(1, cached.get().size());
    Assertions.assertEquals(roleName, cached.get().get(0).name());

    // Simulate revokePrivilegesFromRole: invalidate the role entity
    cache.invalidate(roleIdent, Entity.EntityType.ROLE);

    // After invalidation the METADATA_OBJECT_ROLE_REL cache for the schema must be gone
    Optional<List<RoleEntity>> afterRevoke =
        cache.getIfPresent(
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
            schemaIdent,
            Entity.EntityType.SCHEMA);
    Assertions.assertFalse(
        afterRevoke.isPresent(),
        "METADATA_OBJECT_ROLE_REL cache must be invalidated after revoking the role");
  }

  /**
   * When the role entity was also separately cached (via a previous get), invalidating it must
   * still propagate to the METADATA_OBJECT_ROLE_REL relation cache.
   */
  @Test
  void testRoleInvalidationWithSeparatelyCachedRoleEntity() {
    String metalake = "metalake";
    String catalogName = "catalog";
    String schemaName = "schema1";
    String roleName = "role1";

    RoleEntity role = buildRoleWithSchemaObject(metalake, roleName, catalogName, schemaName);
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalogName, schemaName);
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(metalake, roleName);

    // Simulate separate entity get (e.g., loadRole)
    cache.put(role);

    // Simulate relation cache populated after listBindingRoleNames
    cache.put(
        schemaIdent,
        Entity.EntityType.SCHEMA,
        SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
        Lists.newArrayList(role));

    // Both caches are present
    Assertions.assertTrue(cache.contains(roleIdent, Entity.EntityType.ROLE));
    Assertions.assertTrue(
        cache.contains(
            schemaIdent,
            Entity.EntityType.SCHEMA,
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL));

    // Invalidate role (simulates revokePrivilegesFromRole)
    cache.invalidate(roleIdent, Entity.EntityType.ROLE);

    // Both the role entity cache and the relation cache must be gone
    Assertions.assertFalse(
        cache.contains(roleIdent, Entity.EntityType.ROLE),
        "Role entity cache must be gone after invalidation");
    Assertions.assertFalse(
        cache.contains(
            schemaIdent,
            Entity.EntityType.SCHEMA,
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL),
        "METADATA_OBJECT_ROLE_REL cache must be invalidated after revoking the role");
  }

  /**
   * When multiple roles share the same schema as a securable object, invalidating one role must
   * invalidate the METADATA_OBJECT_ROLE_REL cache for that schema (since the full list is stale).
   *
   * <p>Note: The existing BFS propagation logic also cascades from the cleared relation cache back
   * to other role entities via the schema's reverse index (the same behaviour that clears stale
   * role-entity caches on schema rename). Clearing role2's entity cache is a pre-existing
   * performance trade-off, not a correctness bug: role2 will be re-fetched fresh from the DB on the
   * next access.
   */
  @Test
  void testMultipleRolesInvalidationForSameSchema() {
    String metalake = "metalake";
    String catalogName = "catalog";
    String schemaName = "schema";

    RoleEntity role1 = buildRoleWithSchemaObject(metalake, "role1", catalogName, schemaName);
    RoleEntity role2 = buildRoleWithSchemaObject(metalake, "role2", catalogName, schemaName);

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalogName, schemaName);
    NameIdentifier role1Ident = NameIdentifierUtil.ofRole(metalake, "role1");

    // Cache both role entities separately and the relation list
    cache.put(role1);
    cache.put(role2);
    cache.put(
        schemaIdent,
        Entity.EntityType.SCHEMA,
        SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
        Lists.newArrayList(role1, role2));

    Assertions.assertTrue(
        cache.contains(
            schemaIdent,
            Entity.EntityType.SCHEMA,
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL));

    // Revoke role1
    cache.invalidate(role1Ident, Entity.EntityType.ROLE);

    // The METADATA_OBJECT_ROLE_REL cache must be gone — this is the core correctness fix
    Assertions.assertFalse(
        cache.contains(
            schemaIdent,
            Entity.EntityType.SCHEMA,
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL),
        "Relation cache must be cleared when any of its roles is revoked");
  }

  /**
   * When a role has securable objects on multiple schemas, invalidating the role must invalidate
   * the METADATA_OBJECT_ROLE_REL caches for ALL schemas.
   */
  @Test
  void testRoleWithMultipleSchemasInvalidatesAllRelationCaches() {
    String metalake = "metalake";
    String catalogName = "catalog";
    String roleName = "multi_schema_role";

    SecurableObject cat = SecurableObjects.ofCatalog(catalogName, Lists.newArrayList());
    SecurableObject schema1Obj =
        SecurableObjects.ofSchema(cat, "schema1", Lists.newArrayList(Privileges.UseSchema.allow()));
    SecurableObject schema2Obj =
        SecurableObjects.ofSchema(cat, "schema2", Lists.newArrayList(Privileges.UseSchema.allow()));

    RoleEntity role =
        RoleEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(roleName)
            .withNamespace(AuthorizationUtils.ofRoleNamespace(metalake))
            .withProperties(null)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(schema1Obj, schema2Obj))
            .build();

    NameIdentifier schema1Ident = NameIdentifier.of(metalake, catalogName, "schema1");
    NameIdentifier schema2Ident = NameIdentifier.of(metalake, catalogName, "schema2");
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(metalake, roleName);

    // Cache METADATA_OBJECT_ROLE_REL for both schemas
    cache.put(
        schema1Ident,
        Entity.EntityType.SCHEMA,
        SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
        Lists.newArrayList(role));
    cache.put(
        schema2Ident,
        Entity.EntityType.SCHEMA,
        SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
        Lists.newArrayList(role));

    Assertions.assertTrue(
        cache.contains(
            schema1Ident,
            Entity.EntityType.SCHEMA,
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL));
    Assertions.assertTrue(
        cache.contains(
            schema2Ident,
            Entity.EntityType.SCHEMA,
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL));

    // Revoke role (removes its last privilege on all schemas)
    cache.invalidate(roleIdent, Entity.EntityType.ROLE);

    Assertions.assertFalse(
        cache.contains(
            schema1Ident,
            Entity.EntityType.SCHEMA,
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL),
        "schema1 METADATA_OBJECT_ROLE_REL cache must be cleared");
    Assertions.assertFalse(
        cache.contains(
            schema2Ident,
            Entity.EntityType.SCHEMA,
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL),
        "schema2 METADATA_OBJECT_ROLE_REL cache must be cleared");
  }

  /**
   * When the METADATA_OBJECT_ROLE_REL cache already contains the role (e.g., role was granted
   * earlier and the relation was cached), granting an additional privilege (invalidates the role)
   * must also clear the stale relation cache. This is symmetric with the revoke path.
   */
  @Test
  void testGrantPathInvalidatesRelationCacheWhenRoleWasPreviouslyCached() {
    String metalake = "metalake";
    String catalogName = "catalog";
    String schemaName = "schema_grant";
    String roleName = "grant_role";

    RoleEntity role = buildRoleWithSchemaObject(metalake, roleName, catalogName, schemaName);
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalogName, schemaName);
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(metalake, roleName);

    // Simulate a prior listBindingRoleNames that already included the role
    cache.put(
        schemaIdent,
        Entity.EntityType.SCHEMA,
        SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
        Lists.newArrayList(role));

    // Simulate grantPrivilegesToRole adding another privilege → role entity is updated and
    // invalidated
    cache.invalidate(roleIdent, Entity.EntityType.ROLE);

    // Relation cache should be gone, forcing a fresh DB query on next listBindingRoleNames
    Optional<List<RoleEntity>> afterGrant =
        cache.getIfPresent(
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
            schemaIdent,
            Entity.EntityType.SCHEMA);
    Assertions.assertFalse(
        afterGrant.isPresent(),
        "METADATA_OBJECT_ROLE_REL cache must be cleared after grant so next read returns fresh data");
  }

  /**
   * Verify the reverse index is cleaned up correctly after invalidation, preventing memory leaks.
   */
  @Test
  void testReverseIndexCleanupAfterRoleInvalidation() {
    String metalake = "metalake";
    String catalogName = "catalog";
    String schemaName = "schema_cleanup";
    String roleName = "cleanup_role";

    RoleEntity role = buildRoleWithSchemaObject(metalake, roleName, catalogName, schemaName);
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalogName, schemaName);
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(metalake, roleName);

    cache.put(role);
    cache.put(
        schemaIdent,
        Entity.EntityType.SCHEMA,
        SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
        Lists.newArrayList(role));

    long sizeBeforeInvalidation = cache.size();
    cache.invalidate(roleIdent, Entity.EntityType.ROLE);
    long sizeAfterInvalidation = cache.size();

    // Both the role entity entry and the relation entry should be removed
    Assertions.assertTrue(
        sizeAfterInvalidation < sizeBeforeInvalidation,
        "Cache size must decrease after invalidating role and its related relation entries");

    // Reverse index for role should be empty
    ReverseIndexCache reverseIndex = cache.getReverseIndex();
    List<EntityCacheKey> roleReverseKeys = reverseIndex.get(roleIdent, Entity.EntityType.ROLE);
    Assertions.assertNull(
        roleReverseKeys, "Reverse index for role should be empty after invalidation");
  }
}
