/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.TestJDBCBackend;
import com.datastrato.gravitino.storage.relational.mapper.RoleMetaMapper;
import com.datastrato.gravitino.storage.relational.po.RolePO;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestUserMetaService extends TestJDBCBackend {

  String metalakeName = "metalake";

  @Test
  void getUserByIdentifier() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // get not exist user
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            userMetaService.getUserByIdentifier(AuthorizationUtils.ofUser(metalakeName, "user1")));

    // get user
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo);
    userMetaService.insertUser(user1, false);
    Assertions.assertEquals(user1, userMetaService.getUserByIdentifier(user1.nameIdentifier()));

    // get user with roles
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList("role1", "role2"));
    userMetaService.insertUser(user2, false);
    UserEntity actualUser = userMetaService.getUserByIdentifier(user2.nameIdentifier());
    Assertions.assertEquals(user2.name(), actualUser.name());
    Assertions.assertEquals(
        Sets.newHashSet(user2.roleNames()), Sets.newHashSet(actualUser.roleNames()));
  }

  @Test
  void insertUser() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // insert user
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user1, false));
    Assertions.assertEquals(user1, userMetaService.getUserByIdentifier(user1.nameIdentifier()));

    // insert duplicate user
    UserEntity user1Exist =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo);
    Assertions.assertThrows(
        AlreadyExistsException.class, () -> userMetaService.insertUser(user1Exist, false));

    // insert overwrite
    UserEntity user1Overwrite =
        createUserEntity(
            user1.id(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1Overwrite",
            auditInfo);
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user1Overwrite, true));
    Assertions.assertEquals(
        "user1Overwrite",
        userMetaService.getUserByIdentifier(user1Overwrite.nameIdentifier()).name());
    Assertions.assertEquals(
        user1Overwrite, userMetaService.getUserByIdentifier(user1Overwrite.nameIdentifier()));

    // insert user with roles
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList("role1", "role2"));
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user2, false));
    UserEntity actualUser = userMetaService.getUserByIdentifier(user2.nameIdentifier());
    Assertions.assertEquals(user2.name(), actualUser.name());
    Assertions.assertEquals(
        Sets.newHashSet(user2.roleNames()), Sets.newHashSet(actualUser.roleNames()));

    // insert duplicate user with roles
    UserEntity user2Exist =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo);
    Assertions.assertThrows(
        AlreadyExistsException.class, () -> userMetaService.insertUser(user2Exist, false));

    // insert overwrite user with roles
    UserEntity user2Overwrite =
        createUserEntity(
            user1.id(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2Overwrite",
            auditInfo);
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user2Overwrite, true));
    Assertions.assertEquals(
        "user2Overwrite",
        userMetaService.getUserByIdentifier(user2Overwrite.nameIdentifier()).name());
    Assertions.assertEquals(
        user2Overwrite, userMetaService.getUserByIdentifier(user2Overwrite.nameIdentifier()));
  }

  @Test
  void deleteUser() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // delete user
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> userMetaService.insertUser(user1, false));
    Assertions.assertEquals(user1, userMetaService.getUserByIdentifier(user1.nameIdentifier()));
    Assertions.assertTrue(userMetaService.deleteUser(user1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user1.nameIdentifier()));

    // delete user with roles
    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    UserEntity user2 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user2",
            auditInfo,
            Lists.newArrayList("role1", "role2"));
    userMetaService.insertUser(user2, false);
    List<RolePO> rolePOs =
        SessionUtils.doWithCommitAndFetchResult(
            RoleMetaMapper.class, mapper -> mapper.listRolesByUserId(user2.id()));
    Assertions.assertEquals(2, rolePOs.size());
    UserEntity actualUser = userMetaService.getUserByIdentifier(user2.nameIdentifier());
    Assertions.assertEquals(user2.name(), actualUser.name());
    Assertions.assertEquals(
        Sets.newHashSet(user2.roleNames()), Sets.newHashSet(actualUser.roleNames()));

    Assertions.assertTrue(userMetaService.deleteUser(user2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> userMetaService.getUserByIdentifier(user2.nameIdentifier()));
    rolePOs =
        SessionUtils.doWithCommitAndFetchResult(
            RoleMetaMapper.class, mapper -> mapper.listRolesByUserId(user2.id()));
    Assertions.assertEquals(0, rolePOs.size());
  }

  @Test
  void updateUser() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    UserMetaService userMetaService = UserMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    RoleEntity role1 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role1",
            auditInfo);
    RoleEntity role2 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role2",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    UserEntity user1 =
        createUserEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "user1",
            auditInfo,
            Lists.newArrayList("role1", "role2"));
    userMetaService.insertUser(user1, false);
    UserEntity actualUser = userMetaService.getUserByIdentifier(user1.nameIdentifier());
    Assertions.assertEquals(user1.name(), actualUser.name());
    Assertions.assertEquals(
        Sets.newHashSet(user1.roleNames()), Sets.newHashSet(actualUser.roleNames()));

    RoleEntity role3 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role3",
            auditInfo);
    roleMetaService.insertRole(role3, false);

    // update user (grant)
    Function<UserEntity, UserEntity> grantUpdater =
        user -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(user.auditInfo().creator())
                  .withCreateTime(user.auditInfo().createTime())
                  .withLastModifier("grantUser")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(user.roleNames());
          List<Long> roleIds = Lists.newArrayList(user.roleIds());
          roleNames.add(role3.name());
          roleIds.add(role3.id());

          return UserEntity.builder()
              .withNamespace(user.namespace())
              .withId(user.id())
              .withName(user.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };

    Assertions.assertNotNull(userMetaService.updateUser(user1.nameIdentifier(), grantUpdater));
    UserEntity grantUser =
        UserMetaService.getInstance().getUserByIdentifier(user1.nameIdentifier());
    Assertions.assertEquals(user1.id(), grantUser.id());
    Assertions.assertEquals(user1.name(), grantUser.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role2", "role3"), Sets.newHashSet(grantUser.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role2.id(), role3.id()), Sets.newHashSet(grantUser.roleIds()));
    Assertions.assertEquals("creator", grantUser.auditInfo().creator());
    Assertions.assertEquals("grantUser", grantUser.auditInfo().lastModifier());

    // update user (revoke)
    Function<UserEntity, UserEntity> revokeUpdater =
        user -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(user.auditInfo().creator())
                  .withCreateTime(user.auditInfo().createTime())
                  .withLastModifier("revokeUser")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(user.roleNames());
          List<Long> roleIds = Lists.newArrayList(user.roleIds());
          roleIds.remove(roleNames.indexOf("role2"));
          roleNames.remove("role2");

          return UserEntity.builder()
              .withNamespace(user.namespace())
              .withId(user.id())
              .withName(user.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };

    Assertions.assertNotNull(userMetaService.updateUser(user1.nameIdentifier(), revokeUpdater));
    UserEntity revokeUser =
        UserMetaService.getInstance().getUserByIdentifier(user1.nameIdentifier());
    Assertions.assertEquals(user1.id(), revokeUser.id());
    Assertions.assertEquals(user1.name(), revokeUser.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role3"), Sets.newHashSet(revokeUser.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role3.id()), Sets.newHashSet(revokeUser.roleIds()));
    Assertions.assertEquals("creator", revokeUser.auditInfo().creator());
    Assertions.assertEquals("revokeUser", revokeUser.auditInfo().lastModifier());
  }
}
