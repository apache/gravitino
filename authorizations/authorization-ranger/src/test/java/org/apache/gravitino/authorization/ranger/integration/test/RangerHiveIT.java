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
package org.apache.gravitino.authorization.ranger.integration.test;

import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.currentFunName;
import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.rangerClient;
import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.verifyRoleInRanger;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin;
import org.apache.gravitino.authorization.ranger.RangerHadoopSQLMetadataObject;
import org.apache.gravitino.authorization.ranger.RangerHadoopSQLSecurableObject;
import org.apache.gravitino.authorization.ranger.RangerHelper;
import org.apache.gravitino.authorization.ranger.RangerPrivileges;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerHiveIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveIT.class);

  private static RangerAuthorizationPlugin rangerAuthHivePlugin;
  private static RangerHelper rangerHelper;
  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  @BeforeAll
  public static void setup() {
    RangerITEnv.init(true);

    rangerAuthHivePlugin = RangerITEnv.rangerAuthHivePlugin;
    rangerHelper = RangerITEnv.rangerHelper;
  }

  @AfterAll
  public static void stop() {
    RangerITEnv.cleanup();
  }

  @AfterEach
  public void clean() {
    RangerITEnv.cleanAllPolicy(RangerITEnv.RANGER_HIVE_REPO_NAME);
  }

  /**
   * Create a mock role with 3 securable objects <br>
   * 1. catalog.db1.tab1 with CREATE_TABLE privilege. <br>
   * 2. catalog.db1.tab2 with SELECT_TABLE privilege. <br>
   * 3. catalog.db1.tab3 with MODIFY_TABLE privilege. <br>
   *
   * @param roleName The name of the role must be unique in this test class
   */
  public RoleEntity mock3TableRole(String roleName) {
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format("catalog.%s", roleName), // use unique db name to avoid conflict
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTable.allow()));

    SecurableObject securableObject2 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab2", roleName),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));

    SecurableObject securableObject3 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab3", roleName),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.allow()));

    return RoleEntity.builder()
        .withId(1L)
        .withName(roleName)
        .withAuditInfo(auditInfo)
        .withSecurableObjects(
            Lists.newArrayList(securableObject1, securableObject2, securableObject3))
        .build();
  }

  // Use the different db.table different privilege to test OnRoleCreated()
  @Test
  public void testOnRoleCreated() {
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    verifyRoleInRanger(rangerAuthHivePlugin, role);
  }

  @Test
  public void testOnDenyRoleCreated() {
    String schemaName = currentFunName();
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format("catalog.%s", schemaName), // use unique db name to avoid conflict
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTable.deny()));

    SecurableObject securableObject2 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab2", schemaName),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.deny()));

    SecurableObject securableObject3 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab3", schemaName),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.deny()));

    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(
                Lists.newArrayList(securableObject1, securableObject2, securableObject3))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    verifyRoleInRanger(rangerAuthHivePlugin, role);
  }

  @Test
  public void testOnMixRoleCreated() {
    String schemaName = currentFunName();
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format("catalog.%s", schemaName), // use unique db name to avoid conflict
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.SelectTable.allow(), Privileges.CreateTable.deny()));

    SecurableObject securableObject2 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab2", schemaName),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow(), Privileges.ModifyTable.deny()));

    SecurableObject securableObject3 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab3", schemaName),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.deny(), Privileges.ModifyTable.allow()));

    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(
                Lists.newArrayList(securableObject1, securableObject2, securableObject3))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    verifyRoleInRanger(rangerAuthHivePlugin, role);
  }

  @Test
  public void testOnRoleCreatedCatalog() {
    Role mockCatalogRole = mockCatalogRole(currentFunName());
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(mockCatalogRole));
    // Check if exist this policy
    assertFindManagedPolicyItems(mockCatalogRole, true);
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(mockCatalogRole));
    assertFindManagedPolicyItems(mockCatalogRole, false);
  }

  @Test
  public void testOnDenyRoleCreatedCatalog() {
    String roleName = currentFunName();
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            currentFunName(),
            SecurableObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.deny()));
    RoleEntity mockCatalogRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(roleName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject1))
            .build();

    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(mockCatalogRole));
    // Check if exist this policy
    assertFindManagedPolicyItems(mockCatalogRole, true);
    mockCatalogRole.securableObjects().stream()
        .forEach(
            securableObject -> {
              rangerAuthHivePlugin.translatePrivilege(securableObject).stream()
                  .forEach(
                      rangerSecurableObject -> {
                        verifyRangerSecurableObjectInRanger(
                            rangerSecurableObject,
                            null,
                            null,
                            null,
                            null,
                            Lists.newArrayList(roleName));
                      });
            });

    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(mockCatalogRole));
    assertFindManagedPolicyItems(mockCatalogRole, false);
  }

  @Test
  public void testOnRoleDeleted() {
    // prepare to create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    assertFindManagedPolicyItems(role, true);

    // delete this role
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(role));
    // Check if the policy is deleted
    assertFindManagedPolicyItems(role, false);
  }

  @Test
  public void testOnRoleDeletedCatalog() {
    // prepare to create a role
    Role mockCatalogRole = mockCatalogRole(currentFunName());

    // delete this role
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(mockCatalogRole));
    // Check if exist this policy
    assertFindManagedPolicyItems(mockCatalogRole, false);
  }

  @Test
  public void testOnRoleDeleted2() {
    // prepare to create a role
    RoleEntity role = mock3TableRole(currentFunName());
    // Set metadata object owner
    role.securableObjects().stream()
        .forEach(
            securableObject -> {
              Assertions.assertTrue(
                  rangerAuthHivePlugin.onOwnerSet(
                      securableObject, null, new MockOwner("user1", Owner.Type.USER)));
            });

    // delete this role
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(role));
    // Because this metaobject has owner, so the policy should not be deleted
    assertFindManagedPolicyItems(role, false);
  }

  @Test
  public void testOnRoleAcquired() {
    RoleEntity role = mock3TableRole(GravitinoITUtils.genRandomName(currentFunName()));
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleAcquired(role));
  }

  /** The metalake role does not to create Ranger policy. Only use it to help test */
  public RoleEntity mockCatalogRole(String roleName) {
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            "catalog",
            SecurableObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(roleName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject1))
            .build();
    return role;
  }

  @Test
  public void testFindManagedPolicy() {
    // Because Ranger support wildcard to match the policy, so we need to test the policy with
    // wildcard
    String dbName = currentFunName();
    createHivePolicy(
        Lists.newArrayList(String.format("%s*", dbName), "*"),
        GravitinoITUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList(String.format("%s*", dbName), "tab*"),
        GravitinoITUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList(String.format("%s3", dbName), "*"),
        GravitinoITUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList(String.format("%s3", dbName), "tab*"),
        GravitinoITUtils.genRandomName(currentFunName()));
    // findManagedPolicy function use precise search, so return null
    AuthorizationSecurableObject rangerSecurableObject =
        rangerAuthHivePlugin.generateAuthorizationSecurableObject(
            ImmutableList.of(String.format("%s3", dbName), "tab1"),
            RangerHadoopSQLMetadataObject.Type.TABLE,
            ImmutableSet.of(
                new RangerPrivileges.RangerHivePrivilegeImpl(
                    RangerPrivileges.RangerHadoopSQLPrivilege.ALL, Privilege.Condition.ALLOW)));
    Assertions.assertNull(rangerHelper.findManagedPolicy(rangerSecurableObject));

    // Add a policy for `db3.tab1`
    createHivePolicy(
        Lists.newArrayList(String.format("%s3", dbName), "tab1"),
        GravitinoITUtils.genRandomName(currentFunName()));
    // findManagedPolicy function use precise search, so return not null
    Assertions.assertNotNull(rangerHelper.findManagedPolicy(rangerSecurableObject));
  }

  @Test
  public void testGravitinoCompatibleWithExistingPolicy() {
    RoleEntity role = mock3TableRole(currentFunName());
    role.securableObjects().stream()
        .forEach(
            securableObject -> {
              Joiner DOT_JOINER = Joiner.on('.');
              List<String> names =
                  Lists.newArrayList(
                      SecurableObjects.DOT_SPLITTER.splitToList(securableObject.fullName()));
              names.remove(0); // remove catalog node
              // Manual create the Ranger Policy
              createHivePolicy(Lists.newArrayList(names), DOT_JOINER.join(names));
            });
    List<String> existingPolicyNames =
        findRoleResourceRelatedPolicies(role).stream()
            .map(RangerPolicy::getName)
            .collect(Collectors.toList());
    rangerAuthHivePlugin.onRoleCreated(role);
    findRoleResourceRelatedPolicies(role)
        .forEach(
            policy -> {
              List<RangerPolicy.RangerPolicyItem> policyItems = new ArrayList<>();
              policyItems.addAll(policy.getPolicyItems());
              policyItems.addAll(policy.getDenyPolicyItems());
              policyItems.addAll(policy.getRowFilterPolicyItems());
              policyItems.addAll(policy.getDataMaskPolicyItems());

              if (existingPolicyNames.contains(policy.getName())) {
                Assertions.assertTrue(
                    policyItems.stream()
                        .anyMatch(
                            item ->
                                !item.getRoles()
                                    .contains(
                                        rangerHelper.generateGravitinoRoleName(role.name()))));
              }
              Assertions.assertTrue(
                  policyItems.stream()
                      .anyMatch(
                          item ->
                              item.getRoles()
                                  .contains(rangerHelper.generateGravitinoRoleName(role.name()))));
            });

    rangerAuthHivePlugin.onRoleDeleted(role);
    findRoleResourceRelatedPolicies(role)
        .forEach(
            policy -> {
              List<RangerPolicy.RangerPolicyItem> policyItems = new ArrayList<>();
              policyItems.addAll(policy.getPolicyItems());
              policyItems.addAll(policy.getDenyPolicyItems());
              policyItems.addAll(policy.getRowFilterPolicyItems());
              policyItems.addAll(policy.getDataMaskPolicyItems());

              if (existingPolicyNames.contains(policy.getName())) {
                Assertions.assertFalse(
                    policyItems.stream()
                        .anyMatch(
                            item ->
                                item.getRoles()
                                    .contains(
                                        rangerHelper.generateGravitinoRoleName(role.name()))));
                Assertions.assertTrue(
                    policyItems.stream()
                        .anyMatch(
                            item ->
                                !item.getRoles()
                                    .contains(
                                        rangerHelper.generateGravitinoRoleName(role.name()))));
              } else {
                Assertions.assertEquals(0, policyItems.size());
              }
            });
  }

  static void createHivePolicy(List<String> metaObjects, String roleName) {
    Assertions.assertTrue(metaObjects.size() < 4);
    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap = new HashMap<>();
    for (int i = 0; i < metaObjects.size(); i++) {
      RangerPolicy.RangerPolicyResource policyResource =
          new RangerPolicy.RangerPolicyResource(metaObjects.get(i));
      policyResourceMap.put(
          i == 0
              ? RangerITEnv.RESOURCE_DATABASE
              : i == 1 ? RangerITEnv.RESOURCE_TABLE : RangerITEnv.RESOURCE_COLUMN,
          policyResource);
    }

    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setGroups(Arrays.asList(RangerDefines.PUBLIC_GROUP));
    policyItem.setAccesses(
        Arrays.asList(
            new RangerPolicy.RangerPolicyItemAccess(
                RangerPrivileges.RangerHadoopSQLPrivilege.SELECT.toString())));
    RangerITEnv.updateOrCreateRangerPolicy(
        RangerDefines.SERVICE_TYPE_HIVE,
        RangerITEnv.RANGER_HIVE_REPO_NAME,
        roleName,
        policyResourceMap,
        Collections.singletonList(policyItem));
  }

  static boolean deleteHivePolicy(RangerHadoopSQLSecurableObject rangerSecurableObject) {
    RangerPolicy policy = rangerHelper.findManagedPolicy(rangerSecurableObject);
    if (policy != null) {
      try {
        RangerITEnv.rangerClient.deletePolicy(policy.getId());
      } catch (RangerServiceException e) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testRoleChangeAddSecurableObject() {
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format("catalog.%s.tab1", currentFunName()),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));

    Role mockCatalogRole = mockCatalogRole(currentFunName());
    // 1. Add a securable object to the role
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRoleUpdated(
            mockCatalogRole,
            RoleChange.addSecurableObject(mockCatalogRole.name(), securableObject1)));

    // construct a verified role to check if the role and Ranger policy is created correctly
    RoleEntity verifyRole1 =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject1))
            .build();
    verifyRoleInRanger(rangerAuthHivePlugin, verifyRole1);

    // 2. Multi-call Add a same entity and privilege to the role, because support idempotent
    // operation, so return true
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRoleUpdated(
            mockCatalogRole,
            RoleChange.addSecurableObject(mockCatalogRole.name(), securableObject1)));
    verifyRoleInRanger(rangerAuthHivePlugin, verifyRole1);

    // 3. Add the same metadata but have different privileges to the role
    SecurableObject securableObject3 =
        SecurableObjects.parse(
            securableObject1.fullName(),
            SecurableObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow(), Privileges.ModifyTable.allow()));
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRoleUpdated(
            mockCatalogRole,
            RoleChange.addSecurableObject(mockCatalogRole.name(), securableObject3)));
  }

  @Test
  public void testRoleChangeAddCatalogSecurableObject() {
    Role mockCatalogRole = mockCatalogRole(currentFunName());
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRoleUpdated(
            mockCatalogRole,
            RoleChange.addSecurableObject(
                mockCatalogRole.name(), mockCatalogRole.securableObjects().get(0))));
    assertFindManagedPolicyItems(mockCatalogRole, true);
  }

  @Test
  public void testRoleChangeRemoveSecurableObject() {
    // Prepare a role contain 3 securable objects
    String currentFunName = currentFunName();
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));

    Role mockCatalogRole = mockCatalogRole(currentFunName);
    // remove a securable object from role
    List<SecurableObject> securableObjects = new ArrayList<>(role.securableObjects());
    while (!securableObjects.isEmpty()) {
      SecurableObject removeSecurableObject = securableObjects.remove(0);
      Assertions.assertTrue(
          rangerAuthHivePlugin.onRoleUpdated(
              mockCatalogRole,
              RoleChange.removeSecurableObject(mockCatalogRole.name(), removeSecurableObject)));

      if (!securableObjects.isEmpty()) {
        // Doesn't verify the last securable object, because the role is empty
        RoleEntity verifyRole =
            RoleEntity.builder()
                .withId(1L)
                .withName(currentFunName())
                .withAuditInfo(auditInfo)
                .withSecurableObjects(securableObjects)
                .build();
        verifyRoleInRanger(rangerAuthHivePlugin, verifyRole);
      }
    }
  }

  @Test
  public void testRoleChangeRemoveCatalogSecurableObject() {
    String currentFunName = currentFunName();
    Role mockCatalogRole = mockCatalogRole(currentFunName);

    Assertions.assertTrue(
        rangerAuthHivePlugin.onRoleUpdated(
            mockCatalogRole,
            RoleChange.removeSecurableObject(
                mockCatalogRole.name(), mockCatalogRole.securableObjects().get(0))));
    assertFindManagedPolicyItems(mockCatalogRole, false);
  }

  @Test
  public void testRoleChangeUpdateSecurableObject() {
    SecurableObject oldSecurableObject =
        SecurableObjects.parse(
            String.format("catalog1.%s", currentFunName()),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(oldSecurableObject))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));

    // Keep the same matedata namespace and type, but change privileges
    SecurableObject newSecurableObject =
        SecurableObjects.parse(
            oldSecurableObject.fullName(),
            oldSecurableObject.type(),
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRoleUpdated(
            role,
            RoleChange.updateSecurableObject(role.name(), oldSecurableObject, newSecurableObject)));

    // construct a verified role to check if the role and Ranger policy are created correctly
    RoleEntity verifyRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(newSecurableObject))
            .build();
    verifyRoleInRanger(rangerAuthHivePlugin, verifyRole);
  }

  @Test
  public void testRoleChangeUpdateDifferentMetadata() {
    // Test same metadata namespace and different type
    SecurableObject metalake =
        SecurableObjects.parse(
            String.format("%s", currentFunName()),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    SecurableObject catalog =
        SecurableObjects.parse(
            metalake.fullName(),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(metalake))
            .build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            rangerAuthHivePlugin.onRoleUpdated(
                role, RoleChange.updateSecurableObject(role.name(), metalake, catalog)));

    // Test different metadata namespace and same type
    SecurableObject schema1 =
        SecurableObjects.parse(
            String.format("catalog1.%s", currentFunName()),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    SecurableObject schema2 =
        SecurableObjects.parse(
            String.format("catalog1-diff.%s", currentFunName()),
            schema1.type(),
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            rangerAuthHivePlugin.onRoleUpdated(
                role, RoleChange.updateSecurableObject(role.name(), schema1, schema2)));
  }

  @Test
  public void testRoleChangeUpdateCatalogSecurableObject() {
    String currentFunName = currentFunName();
    Role mockCatalogRole = mockCatalogRole(currentFunName);

    // Keep the same matedata namespace and type, but change privileges
    SecurableObject newSecurableObject =
        SecurableObjects.parse(
            mockCatalogRole.securableObjects().get(0).fullName(),
            mockCatalogRole.securableObjects().get(0).type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onRoleUpdated(
            mockCatalogRole,
            RoleChange.updateSecurableObject(
                mockCatalogRole.name(),
                mockCatalogRole.securableObjects().get(0),
                newSecurableObject)));
    assertFindManagedPolicyItems(mockCatalogRole, false);
  }

  @Test
  public void testMetadataObjectChangeRenameMetalake() {
    String currentFunName = currentFunName();
    MetadataObject oldMetadataObject =
        MetadataObjects.parse(
            String.format("metalake-%s", currentFunName), MetadataObject.Type.METALAKE);
    SecurableObject oldSecurableObject1 =
        SecurableObjects.parse(
            oldMetadataObject.fullName(),
            oldMetadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(oldSecurableObject1))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    assertFindManagedPolicyItems(role, true);

    String newMetalake = String.format("metalake-new-%s", currentFunName);
    MetadataObject newMetadataObject = MetadataObjects.parse(newMetalake, oldMetadataObject.type());
    Assertions.assertTrue(
        rangerAuthHivePlugin.onMetadataUpdated(
            MetadataObjectChange.rename(oldMetadataObject, newMetadataObject)));
    SecurableObject newSecurableObject1 =
        SecurableObjects.parse(
            newMetadataObject.fullName(),
            newMetadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity newRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(newSecurableObject1))
            .build();
    assertFindManagedPolicyItems(newRole, true);
    Assertions.assertEquals(newMetalake, rangerAuthHivePlugin.getMetalake());
  }

  @Test
  public void testMetadataObjectChangeRenameCatalog() {
    String currentFunName = currentFunName();
    MetadataObject oldMetadataObject =
        MetadataObjects.parse(
            String.format("catalog-%s", currentFunName), MetadataObject.Type.CATALOG);
    SecurableObject oldSecurableObject1 =
        SecurableObjects.parse(
            oldMetadataObject.fullName(),
            oldMetadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(oldSecurableObject1))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    assertFindManagedPolicyItems(role, true);

    MetadataObject newMetadataObject =
        MetadataObjects.parse(
            String.format("catalog-new-%s", currentFunName), oldMetadataObject.type());
    Assertions.assertTrue(
        rangerAuthHivePlugin.onMetadataUpdated(
            MetadataObjectChange.rename(oldMetadataObject, newMetadataObject)));
    SecurableObject newSecurableObject1 =
        SecurableObjects.parse(
            newMetadataObject.fullName(),
            newMetadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity newRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(newSecurableObject1))
            .build();
    assertFindManagedPolicyItems(newRole, true);
  }

  @Test
  public void testMetadataObjectChangeRenameSchema() {
    String currentFunName = currentFunName();
    MetadataObject oldMetadataObject =
        MetadataObjects.parse(
            String.format("catalog.old-%s", currentFunName), MetadataObject.Type.SCHEMA);
    SecurableObject oldSecurableObject1 =
        SecurableObjects.parse(
            oldMetadataObject.fullName(),
            oldMetadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(oldSecurableObject1))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    assertFindManagedPolicyItems(role, true);

    MetadataObject newMetadataObject =
        MetadataObjects.parse(
            String.format("catalog.new-%s", currentFunName), oldMetadataObject.type());
    Assertions.assertTrue(
        rangerAuthHivePlugin.onMetadataUpdated(
            MetadataObjectChange.rename(oldMetadataObject, newMetadataObject)));
    assertFindManagedPolicyItems(role, false);
    SecurableObject newSecurableObject1 =
        SecurableObjects.parse(
            newMetadataObject.fullName(),
            newMetadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity newRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(newSecurableObject1))
            .build();
    assertFindManagedPolicyItems(newRole, true);
  }

  @Test
  public void testMetadataObjectChangeRenameTable() {
    String currentFunName = currentFunName();
    MetadataObject oldMetadataObject =
        MetadataObjects.parse(
            String.format("catalog.schema1.old-%s", currentFunName), MetadataObject.Type.TABLE);
    SecurableObject oldSecurableObject1 =
        SecurableObjects.parse(
            oldMetadataObject.fullName(),
            oldMetadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(oldSecurableObject1))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    assertFindManagedPolicyItems(role, true);

    MetadataObject newMetadataObject =
        MetadataObjects.parse(
            String.format("catalog.schema1.new-%s", currentFunName), oldMetadataObject.type());
    Assertions.assertTrue(
        rangerAuthHivePlugin.onMetadataUpdated(
            MetadataObjectChange.rename(oldMetadataObject, newMetadataObject)));
    assertFindManagedPolicyItems(role, false);
    SecurableObject newSecurableObject1 =
        SecurableObjects.parse(
            newMetadataObject.fullName(),
            newMetadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity newRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(newSecurableObject1))
            .build();
    assertFindManagedPolicyItems(newRole, true);
  }

  @Test
  public void testMetadataObjectChangeRemoveMetalake() throws RangerServiceException {
    metadataObjectChangeRemoveMetalakeOrCatalog(currentFunName(), MetadataObject.Type.METALAKE);
  }

  @Test
  public void testMetadataObjectChangeRemoveCatalog() throws RangerServiceException {
    metadataObjectChangeRemoveMetalakeOrCatalog(currentFunName(), MetadataObject.Type.CATALOG);
  }

  void metadataObjectChangeRemoveMetalakeOrCatalog(String funcName, MetadataObject.Type type)
      throws RangerServiceException {
    createHivePolicy(
        Lists.newArrayList(String.format("%s*", funcName), "*"),
        GravitinoITUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList(String.format("%s*", funcName), "tab*"),
        GravitinoITUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList(String.format("%s3", funcName), "*"),
        GravitinoITUtils.genRandomName(currentFunName()));
    createHivePolicy(
        Lists.newArrayList(String.format("%s3", funcName), "tab*"),
        GravitinoITUtils.genRandomName(currentFunName()));
    Assertions.assertEquals(
        4, rangerClient.getPoliciesInService(RangerITEnv.RANGER_HIVE_REPO_NAME).size());

    String currentFunName = currentFunName();
    MetadataObject metadataObject =
        MetadataObjects.parse(String.format("metalake-%s", currentFunName), type);
    SecurableObject oldSecurableObject1 =
        SecurableObjects.parse(
            metadataObject.fullName(),
            metadataObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(oldSecurableObject1))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    assertFindManagedPolicyItems(role, true);

    Assertions.assertTrue(
        rangerAuthHivePlugin.onMetadataUpdated(MetadataObjectChange.remove(metadataObject)));
    assertFindManagedPolicyItems(role, false);

    Assertions.assertEquals(
        6, rangerClient.getPoliciesInService(RangerITEnv.RANGER_HIVE_REPO_NAME).size());
    rangerClient
        .getPoliciesInService(RangerITEnv.RANGER_HIVE_REPO_NAME)
        .forEach(
            policy -> {
              Assertions.assertFalse(rangerHelper.hasGravitinoManagedPolicyItem(policy));
            });
  }

  @Test
  public void testMetadataObjectChangeRemoveSchema() throws RangerServiceException {
    String currentFunName = currentFunName();
    MetadataObject schemaObject =
        MetadataObjects.parse(
            String.format("catalog.old-%s", currentFunName), MetadataObject.Type.SCHEMA);
    SecurableObject schemaSecurableObject =
        SecurableObjects.parse(
            schemaObject.fullName(),
            schemaObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    MetadataObject tableObject =
        MetadataObjects.parse(
            String.format("catalog.old-%s.tab1", currentFunName), MetadataObject.Type.TABLE);
    SecurableObject tableSecurableObject =
        SecurableObjects.parse(
            tableObject.fullName(),
            tableObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(schemaSecurableObject, tableSecurableObject))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    assertFindManagedPolicyItems(role, true);
    Assertions.assertEquals(
        4, rangerClient.getPoliciesInService(RangerITEnv.RANGER_HIVE_REPO_NAME).size());

    Assertions.assertTrue(
        rangerAuthHivePlugin.onMetadataUpdated(MetadataObjectChange.remove(schemaObject)));
    RoleEntity roleVerify =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(schemaSecurableObject))
            .build();
    assertFindManagedPolicyItems(roleVerify, false);
    Assertions.assertEquals(
        4, rangerClient.getPoliciesInService(RangerITEnv.RANGER_HIVE_REPO_NAME).size());
  }

  @Test
  public void testMetadataObjectChangeRemoveTable() throws RangerServiceException {
    String currentFunName = currentFunName();
    MetadataObject schemaObject =
        MetadataObjects.parse(
            String.format("catalog.old-%s", currentFunName), MetadataObject.Type.SCHEMA);
    SecurableObject securableObjectSchema =
        SecurableObjects.parse(
            schemaObject.fullName(),
            schemaObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    MetadataObject tableObject =
        MetadataObjects.parse(
            String.format("catalog.old-%s.tab1", currentFunName), MetadataObject.Type.TABLE);
    SecurableObject securableObjectTab =
        SecurableObjects.parse(
            tableObject.fullName(),
            tableObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObjectSchema, securableObjectTab))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));
    assertFindManagedPolicyItems(role, true);

    Assertions.assertTrue(
        rangerAuthHivePlugin.onMetadataUpdated(MetadataObjectChange.remove(tableObject)));
    RoleEntity verifyScheamRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObjectSchema))
            .build();
    RoleEntity verifyTableRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObjectTab))
            .build();
    assertFindManagedPolicyItems(verifyScheamRole, true);
    assertFindManagedPolicyItems(verifyTableRole, false);
    Assertions.assertEquals(
        4, rangerClient.getPoliciesInService(RangerITEnv.RANGER_HIVE_REPO_NAME).size());
  }

  @Test
  public void testRoleChangeCombinedOperation() {
    MetadataObject oldMetadataObject =
        MetadataObjects.parse(
            String.format("catalog.%s.tab1", currentFunName()), MetadataObject.Type.TABLE);
    String userName = "user1";
    rangerAuthHivePlugin.onOwnerSet(
        oldMetadataObject, null, new MockOwner(userName, Owner.Type.USER));
    rangerAuthHivePlugin.translateOwner(oldMetadataObject).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject, Lists.newArrayList(userName));
            });

    SecurableObject oldSecurableObject =
        SecurableObjects.parse(
            oldMetadataObject.fullName(),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.allow()));

    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(oldSecurableObject))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));

    // Keep the same matedata namespace and type, but change privileges
    SecurableObject newSecurableObject =
        SecurableObjects.parse(
            oldSecurableObject.fullName(),
            oldSecurableObject.type(),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRoleUpdated(
            role,
            RoleChange.updateSecurableObject(role.name(), oldSecurableObject, newSecurableObject)));

    // construct a verified role to check if the role and Ranger policy are created correctly
    RoleEntity verifyRole =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(newSecurableObject))
            .build();
    verifyRoleInRanger(rangerAuthHivePlugin, verifyRole);
    rangerAuthHivePlugin.translateOwner(oldMetadataObject).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject, Lists.newArrayList(userName));
            });

    // Delete the role
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(verifyRole));
    // policy item will be deleted.
    assertFindManagedPolicyItems(role, false);
    rangerAuthHivePlugin.translateOwner(oldMetadataObject).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject, Lists.newArrayList(userName));
            });
  }

  @Test
  public void testOnGrantedRolesToUser() {
    // prepare to create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));

    // granted a role to the user1
    String userName1 = "user1";
    UserEntity userEntity1 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role, Lists.newArrayList(userName1));

    // multi-call to granted role to the user1
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role, Lists.newArrayList(userName1));

    // granted a role to the user2
    String userName2 = "user2";
    UserEntity userEntity2 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName2)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role), userEntity2));

    // Same to verify user1 and user2
    verifyRoleInRanger(rangerAuthHivePlugin, role, Lists.newArrayList(userName1, userName2));
  }

  @Test
  public void testOnRevokedRolesFromUser() {
    // prepare to create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));

    // granted a role to the user1
    String userName1 = "user1";
    UserEntity userEntity1 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role, Lists.newArrayList(userName1));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role, null, Lists.newArrayList(userName1));

    // multi-call to revoked role from user1
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role), userEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role, null, Lists.newArrayList(userName1));
  }

  @Test
  public void testOnGrantedRolesToGroup() {
    // prepare to create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));

    // granted a role to the group1
    String groupName1 = "group1";
    GroupEntity groupEntity1 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role, null, null, Lists.newArrayList(groupName1));

    // multi-call to grant a role to the group1 test idempotent operation
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role, null, null, Lists.newArrayList(groupName1));

    // granted a role to the group2
    String groupName2 = "group2";
    GroupEntity groupEntity2 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName2)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity2));

    // Same to verify group1 and group2
    verifyRoleInRanger(
        rangerAuthHivePlugin, role, null, null, Lists.newArrayList(groupName1, groupName2));
  }

  @Test
  public void testOnRevokedRolesFromGroup() {
    // prepare to create a role
    RoleEntity role = mock3TableRole(currentFunName());
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role));

    // granted a role to the group1
    String groupName1 = "group1";
    GroupEntity groupEntity1 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role, null, null, Lists.newArrayList(groupName1));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(
        rangerAuthHivePlugin, role, null, null, null, Lists.newArrayList(groupName1));

    // multi-call to revoke from the group1
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromGroup(Lists.newArrayList(role), groupEntity1));
    verifyRoleInRanger(
        rangerAuthHivePlugin, role, null, null, null, Lists.newArrayList(groupName1));
  }

  private void assertFindManagedPolicyItems(Role role, boolean gravitinoPolicyItemExist) {
    List<RangerPolicy> policies = findRoleResourceRelatedPolicies(role);

    if (gravitinoPolicyItemExist) {
      Assertions.assertTrue(policies.size() > 0);
    }

    policies.forEach(
        policy -> {
          List<RangerPolicy.RangerPolicyItem> policyItems = new ArrayList<>();
          policyItems.addAll(policy.getPolicyItems());
          policyItems.addAll(policy.getDenyPolicyItems());
          policyItems.addAll(policy.getRowFilterPolicyItems());
          policyItems.addAll(policy.getDataMaskPolicyItems());
          if (gravitinoPolicyItemExist) {
            Assertions.assertTrue(hasGravitinoManagedPolicyItemAccess(policyItems, role.name()));
          } else {
            Assertions.assertFalse(hasGravitinoManagedPolicyItemAccess(policyItems, role.name()));
          }
        });
  }

  private List<RangerPolicy> findRoleResourceRelatedPolicies(Role role) {
    return role.securableObjects().stream()
        .flatMap(
            securableObject ->
                rangerAuthHivePlugin.translatePrivilege(securableObject).stream()
                    .map(
                        rangerSecurableObject -> {
                          LOG.info("rangerSecurableObject: " + rangerSecurableObject);
                          return rangerHelper.findManagedPolicy(rangerSecurableObject);
                        }))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  public boolean hasGravitinoManagedPolicyItemAccess(
      List<RangerPolicy.RangerPolicyItem> items, String roleName) {
    return items.stream()
        .anyMatch(
            item ->
                item.getRoles().stream()
                    .anyMatch(r -> r.equals(rangerHelper.generateGravitinoRoleName(roleName))));
  }

  private static class MockOwner implements Owner {
    private final String name;
    private final Type type;

    public MockOwner(String name, Type type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type type() {
      return type;
    }
  }

  @Test
  public void testOnOwnerSet() {
    MetadataObject schema =
        MetadataObjects.parse(
            String.format("catalog.%s", currentFunName()), MetadataObject.Type.SCHEMA);
    String userName1 = "user1";
    Owner owner1 = new MockOwner(userName1, Owner.Type.USER);
    Assertions.assertTrue(rangerAuthHivePlugin.onOwnerSet(schema, null, owner1));
    rangerAuthHivePlugin.translateOwner(schema).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject, Lists.newArrayList(userName1));
            });

    MetadataObject table =
        MetadataObjects.parse(
            String.format("catalog.%s.tab1", currentFunName()), MetadataObject.Type.TABLE);
    String userName2 = "user2";
    Owner owner2 = new MockOwner(userName2, Owner.Type.USER);
    Assertions.assertTrue(rangerAuthHivePlugin.onOwnerSet(table, null, owner2));
    rangerAuthHivePlugin.translateOwner(table).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject, Lists.newArrayList(userName2));
            });

    String userName3 = "user3";
    Owner owner3 = new MockOwner(userName3, Owner.Type.USER);
    Assertions.assertTrue(rangerAuthHivePlugin.onOwnerSet(table, owner2, owner3));
    rangerAuthHivePlugin.translateOwner(table).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject,
                  Lists.newArrayList(userName3),
                  Lists.newArrayList(userName2));
            });

    String groupName1 = "group1";
    Owner owner4 = new MockOwner(groupName1, Owner.Type.GROUP);
    Assertions.assertTrue(rangerAuthHivePlugin.onOwnerSet(table, owner3, owner4));
    rangerAuthHivePlugin.translateOwner(table).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject,
                  null,
                  Lists.newArrayList(userName1, userName2),
                  Lists.newArrayList(groupName1));
            });

    String groupName2 = "group2";
    Owner owner5 = new MockOwner(groupName2, Owner.Type.GROUP);
    Assertions.assertTrue(rangerAuthHivePlugin.onOwnerSet(table, owner4, owner5));
    rangerAuthHivePlugin.translateOwner(table).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject,
                  null,
                  Lists.newArrayList(userName1, userName2),
                  Lists.newArrayList(groupName2),
                  Lists.newArrayList(groupName1));
            });
  }

  @Test
  public void testOnOwnerSetCatalog() {
    MetadataObject metalake =
        MetadataObjects.parse(
            String.format("metalake-%s", currentFunName()), MetadataObject.Type.METALAKE);
    String userName1 = "user1";
    Owner owner1 = new MockOwner(userName1, Owner.Type.USER);
    Assertions.assertTrue(rangerAuthHivePlugin.onOwnerSet(metalake, null, owner1));
    rangerAuthHivePlugin.translateOwner(metalake).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject,
                  null,
                  null,
                  null,
                  null,
                  Lists.newArrayList(RangerHelper.GRAVITINO_METALAKE_OWNER_ROLE));
            });

    MetadataObject catalog =
        MetadataObjects.parse(
            String.format("catalog-%s", currentFunName()), MetadataObject.Type.CATALOG);
    String userName2 = "user2";
    Owner owner2 = new MockOwner(userName2, Owner.Type.USER);
    Assertions.assertTrue(rangerAuthHivePlugin.onOwnerSet(catalog, null, owner2));
    rangerAuthHivePlugin.translateOwner(catalog).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject,
                  null,
                  null,
                  null,
                  null,
                  Lists.newArrayList(
                      RangerHelper.GRAVITINO_METALAKE_OWNER_ROLE,
                      RangerHelper.GRAVITINO_CATALOG_OWNER_ROLE));
            });
  }

  @Test
  public void testCreateUser() {
    UserEntity user =
        UserEntity.builder()
            .withId(0L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withRoleIds(null)
            .withRoleNames(null)
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onUserAdded(user));
    Assertions.assertTrue(rangerAuthHivePlugin.onUserAcquired(user));
    Assertions.assertTrue(rangerAuthHivePlugin.onUserRemoved(user));
    Assertions.assertFalse(rangerAuthHivePlugin.onUserAcquired(user));
  }

  @Test
  public void testCreateGroup() {
    GroupEntity group =
        GroupEntity.builder()
            .withId(0L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withRoleIds(null)
            .withRoleNames(null)
            .build();

    Assertions.assertTrue(rangerAuthHivePlugin.onGroupAdded(group));
    Assertions.assertTrue(rangerAuthHivePlugin.onGroupAcquired(group));
    Assertions.assertTrue(rangerAuthHivePlugin.onGroupRemoved(group));
    Assertions.assertFalse(rangerAuthHivePlugin.onGroupAcquired(group));
  }

  @Test
  public void testCombinationOperation() {
    // Create a `ModifyTable` privilege role
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format(
                "catalog.%s.tab1", currentFunName()), // use unique db name to avoid conflict
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.allow()));

    String ownerName = "owner1";
    rangerAuthHivePlugin.onOwnerSet(
        securableObject1, null, new MockOwner(ownerName, Owner.Type.USER));
    rangerAuthHivePlugin.translateOwner(securableObject1).stream()
        .forEach(
            rangerSecurableObject -> {
              verifyRangerSecurableObjectInRanger(
                  rangerSecurableObject, Lists.newArrayList(ownerName), null, null, null);
            });

    RoleEntity role1 =
        RoleEntity.builder()
            .withId(1L)
            .withName(GravitinoITUtils.genRandomName(currentFunName()))
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject1))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role1));
    verifyRoleInRanger(rangerAuthHivePlugin, role1);

    // Create a `SelectTable` privilege role
    SecurableObject securableObject2 =
        SecurableObjects.parse(
            securableObject1.fullName(), // Use the same db.table to test the combination operation
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role2 =
        RoleEntity.builder()
            .withId(1L)
            .withName(GravitinoITUtils.genRandomName(currentFunName()))
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject2))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role2));
    verifyRoleInRanger(rangerAuthHivePlugin, role2);

    // Create a `ModifyTable` privilege role
    SecurableObject securableObject3 =
        SecurableObjects.parse(
            securableObject1.fullName(), // Use the same db.table to test the combination operation
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    RoleEntity role3 =
        RoleEntity.builder()
            .withId(1L)
            .withName(GravitinoITUtils.genRandomName(currentFunName()))
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject3))
            .build();
    Assertions.assertTrue(rangerAuthHivePlugin.onRoleCreated(role3));
    verifyRoleInRanger(rangerAuthHivePlugin, role3);

    /** Test grant to user */
    // granted role1 to the user1
    String userName1 = "user1";
    UserEntity userEntity1 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role1), userEntity1));
    // multiple call to grant role1 to the user1 to test idempotent operation
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role1), userEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role1, Lists.newArrayList(userName1));

    // granted role1 to the user2
    String userName2 = "user2";
    UserEntity userEntity2 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName2)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role1), userEntity2));
    verifyRoleInRanger(rangerAuthHivePlugin, role1, Lists.newArrayList(userName1, userName2));

    // granted role1 to the user3
    String userName3 = "user3";
    UserEntity userEntity3 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName3)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role1), userEntity3));
    verifyRoleInRanger(
        rangerAuthHivePlugin, role1, Lists.newArrayList(userName1, userName2, userName3));

    // Same granted role2 and role3 to the user1 and user2 and user3
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role2, role3), userEntity1));
    verifyRoleInRanger(rangerAuthHivePlugin, role2, Lists.newArrayList(userName1));
    verifyRoleInRanger(rangerAuthHivePlugin, role3, Lists.newArrayList(userName1));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role2, role3), userEntity2));
    verifyRoleInRanger(rangerAuthHivePlugin, role2, Lists.newArrayList(userName1, userName2));
    verifyRoleInRanger(rangerAuthHivePlugin, role3, Lists.newArrayList(userName1, userName2));
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToUser(Lists.newArrayList(role2, role3), userEntity3));
    verifyRoleInRanger(
        rangerAuthHivePlugin, role2, Lists.newArrayList(userName1, userName2, userName3));
    verifyRoleInRanger(
        rangerAuthHivePlugin, role3, Lists.newArrayList(userName1, userName2, userName3));

    /** Test grant to group */
    // granted role1 to the group1
    String groupName1 = "group1";
    GroupEntity groupEntity1 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role1), groupEntity1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1));

    // granted role1 to the group2
    String groupName2 = "group2";
    GroupEntity groupEntity2 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName2)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role1), groupEntity2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2));

    // granted role1 to the group3
    String groupName3 = "group3";
    GroupEntity groupEntity3 =
        GroupEntity.builder()
            .withId(1L)
            .withName(groupName3)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role1), groupEntity3));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2, groupName3));

    // Same granted role2 and role3 to the group1 and group2 and group3
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role2, role3), groupEntity1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role2,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role3,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role2, role3), groupEntity2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role2,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role3,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2));
    Assertions.assertTrue(
        rangerAuthHivePlugin.onGrantedRolesToGroup(Lists.newArrayList(role2, role3), groupEntity3));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role2,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2, groupName3));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role3,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2, groupName3));

    /** Test revoke from user */
    // revoke role1 from the user1
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role1), userEntity1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        Lists.newArrayList(userName2, userName3),
        Lists.newArrayList(userName1),
        Lists.newArrayList(groupName1, groupName2, groupName3));

    // revoke role1 from the user2
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role1), userEntity2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        Lists.newArrayList(userName3),
        Lists.newArrayList(userName1, userName2),
        Lists.newArrayList(groupName1, groupName2, groupName3));

    // revoke role1 from the user3
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role1), userEntity3));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        Lists.newArrayList(groupName1, groupName2, groupName3));

    // Same revoke role2 and role3 from the user1 and user2 and user3
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role2, role3), userEntity1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role2,
        Lists.newArrayList(userName2, userName3),
        Lists.newArrayList(userName1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role3,
        Lists.newArrayList(userName2, userName3),
        Lists.newArrayList(userName1));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role2, role3), userEntity2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role2,
        Lists.newArrayList(userName3),
        Lists.newArrayList(userName1, userName2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role3,
        Lists.newArrayList(userName3),
        Lists.newArrayList(userName1, userName2));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role2, role3), userEntity3));
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromUser(Lists.newArrayList(role2, role3), userEntity3));
    verifyRoleInRanger(
        rangerAuthHivePlugin, role2, null, Lists.newArrayList(userName1, userName2, userName3));
    verifyRoleInRanger(
        rangerAuthHivePlugin, role3, null, Lists.newArrayList(userName1, userName2, userName3));

    /** Test revoke from group */
    // revoke role1 from the group1
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromGroup(Lists.newArrayList(role1), groupEntity1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        Lists.newArrayList(groupName2, groupName3),
        Lists.newArrayList(groupName1));

    // revoke role1 from the group2
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromGroup(Lists.newArrayList(role1), groupEntity2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        Lists.newArrayList(groupName3),
        Lists.newArrayList(groupName1, groupName2));

    // revoke role1 from the group3
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromGroup(Lists.newArrayList(role1), groupEntity3));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role1,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2, groupName3));

    // Same revoke role2 and role3 from the group1 and group2 and group3
    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromGroup(
            Lists.newArrayList(role2, role3), groupEntity1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role2,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        Lists.newArrayList(groupName2, groupName3),
        Lists.newArrayList(groupName1));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role3,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        Lists.newArrayList(groupName2, groupName3),
        Lists.newArrayList(groupName1));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromGroup(
            Lists.newArrayList(role2, role3), groupEntity2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role2,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        Lists.newArrayList(groupName3),
        Lists.newArrayList(groupName1, groupName2));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role3,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        Lists.newArrayList(groupName3),
        Lists.newArrayList(groupName1, groupName2));

    Assertions.assertTrue(
        rangerAuthHivePlugin.onRevokedRolesFromGroup(
            Lists.newArrayList(role2, role3), groupEntity3));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role2,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2, groupName3));
    verifyRoleInRanger(
        rangerAuthHivePlugin,
        role3,
        null,
        Lists.newArrayList(userName1, userName2, userName3),
        null,
        Lists.newArrayList(groupName1, groupName2, groupName3));

    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(role1));
    // policy items will be deleted.
    assertFindManagedPolicyItems(role1, false);

    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(role2));
    assertFindManagedPolicyItems(role2, false);

    Assertions.assertTrue(rangerAuthHivePlugin.onRoleDeleted(role3));
    assertFindManagedPolicyItems(role3, false);
  }

  /**
   * Verify the Gravitino role in Ranger service
   *
   * @param rangerSecurableObject: the Ranger securable object to be verified
   */
  private void verifyRangerSecurableObjectInRanger(
      AuthorizationSecurableObject rangerSecurableObject,
      List<String> includeUsers,
      List<String> excludeUsers,
      List<String> includeGroups,
      List<String> excludeGroups,
      List<String> includeRoles,
      List<String> excludeRoles) {
    // Find policy by each metadata Object
    String policyName = rangerSecurableObject.fullName();
    RangerPolicy policy;
    try {
      policy = RangerITEnv.rangerClient.getPolicy(RangerITEnv.RANGER_HIVE_REPO_NAME, policyName);
      LOG.info("policy: " + policy.toString());
    } catch (RangerServiceException e) {
      LOG.error("Failed to get policy: " + policyName);
      throw new RuntimeException(e);
    }

    Assertions.assertEquals(policy.getName(), policyName);

    // verify namespace
    List<String> metaObjNamespaces = rangerSecurableObject.names();
    List<String> rolePolicies = new ArrayList<>();
    for (int i = 0; i < metaObjNamespaces.size(); i++) {
      rolePolicies.add(
          policy
              .getResources()
              .get(
                  i == 0
                      ? RangerITEnv.RESOURCE_DATABASE
                      : i == 1 ? RangerITEnv.RESOURCE_TABLE : RangerITEnv.RESOURCE_COLUMN)
              .getValues()
              .get(0));
    }
    Assertions.assertEquals(metaObjNamespaces, rolePolicies);

    AtomicReference<List<RangerPolicy.RangerPolicyItem>> policyItems = new AtomicReference<>();
    rangerSecurableObject.privileges().stream()
        .forEach(
            privilege -> {
              if (privilege.condition() == Privilege.Condition.ALLOW) {
                policyItems.set(policy.getPolicyItems());
              } else {
                policyItems.set(policy.getDenyPolicyItems());
              }
              policyItems.get().stream()
                  .filter(
                      policyItem -> {
                        // Filter Ranger policy item by Gravitino privilege
                        return policyItem.getAccesses().stream()
                            .anyMatch(
                                access -> {
                                  return rangerAuthHivePlugin
                                      .ownerMappingRule()
                                      .contains(RangerPrivileges.valueOf(access.getType()));
                                });
                      })
                  .allMatch(
                      policyItem -> {
                        if (includeUsers != null && !includeUsers.isEmpty()) {
                          if (!policyItem.getUsers().containsAll(includeUsers)) {
                            return false;
                          }
                        }
                        if (excludeUsers != null && !excludeUsers.isEmpty()) {
                          boolean containExcludeUser =
                              policyItem.getUsers().stream()
                                  .anyMatch(
                                      user -> {
                                        return excludeUsers.contains(user);
                                      });
                          if (containExcludeUser) {
                            return false;
                          }
                        }
                        if (includeGroups != null && !includeGroups.isEmpty()) {
                          if (!policyItem.getGroups().containsAll(includeGroups)) {
                            return false;
                          }
                        }
                        if (excludeGroups != null && !excludeGroups.isEmpty()) {
                          boolean containExcludeGroup =
                              policyItem.getGroups().stream()
                                  .anyMatch(
                                      user -> {
                                        return excludeGroups.contains(user);
                                      });
                          if (containExcludeGroup) {
                            return false;
                          }
                        }
                        if (includeRoles != null && !includeRoles.isEmpty()) {
                          if (!policyItem.getRoles().containsAll(includeRoles)) {
                            return false;
                          }
                        }
                        if (excludeRoles != null && !excludeRoles.isEmpty()) {
                          boolean containExcludeRole =
                              policyItem.getRoles().stream()
                                  .anyMatch(
                                      role -> {
                                        return excludeRoles.contains(role);
                                      });
                          if (containExcludeRole) {
                            return false;
                          }
                        }
                        return true;
                      });
            });
  }

  private void verifyRangerSecurableObjectInRanger(AuthorizationSecurableObject securableObject) {
    verifyRangerSecurableObjectInRanger(securableObject, null, null, null, null, null, null);
  }

  private void verifyRangerSecurableObjectInRanger(
      AuthorizationSecurableObject securableObject, List<String> includeUsers) {
    verifyRangerSecurableObjectInRanger(
        securableObject, includeUsers, null, null, null, null, null);
  }

  private void verifyRangerSecurableObjectInRanger(
      AuthorizationSecurableObject securableObject,
      List<String> includeUsers,
      List<String> excludeUsers) {
    verifyRangerSecurableObjectInRanger(
        securableObject, includeUsers, excludeUsers, null, null, null, null);
  }

  private void verifyRangerSecurableObjectInRanger(
      AuthorizationSecurableObject securableObject,
      List<String> includeUsers,
      List<String> excludeUsers,
      List<String> includeGroups) {
    verifyRangerSecurableObjectInRanger(
        securableObject, includeUsers, excludeUsers, includeGroups, null, null, null);
  }

  private void verifyRangerSecurableObjectInRanger(
      AuthorizationSecurableObject securableObject,
      List<String> includeUsers,
      List<String> excludeUsers,
      List<String> includeGroups,
      List<String> excludeGroups) {
    verifyRangerSecurableObjectInRanger(
        securableObject, includeUsers, excludeUsers, includeGroups, excludeGroups, null, null);
  }

  private void verifyRangerSecurableObjectInRanger(
      AuthorizationSecurableObject securableObject,
      List<String> includeUsers,
      List<String> excludeUsers,
      List<String> includeGroups,
      List<String> excludeGroups,
      List<String> includeRoles) {
    verifyRangerSecurableObjectInRanger(
        securableObject,
        includeUsers,
        excludeUsers,
        includeGroups,
        excludeGroups,
        includeRoles,
        null);
  }
}
