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
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.TestJDBCBackend;
import com.datastrato.gravitino.storage.relational.mapper.RoleMetaMapper;
import com.datastrato.gravitino.storage.relational.po.RolePO;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestGroupMetaService extends TestJDBCBackend {

  String metalakeName = "metalake";

  @Test
  void getGroupByIdentifier() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // get not exist group
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            groupMetaService.getGroupByIdentifier(
                AuthorizationUtils.ofGroup(metalakeName, "group1")));

    // get group
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo);
    groupMetaService.insertGroup(group1, false);
    Assertions.assertEquals(group1, groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));

    // get group with roles
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
    GroupEntity group2 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group2",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    groupMetaService.insertGroup(group2, false);
    GroupEntity actualGroup = groupMetaService.getGroupByIdentifier(group2.nameIdentifier());
    Assertions.assertEquals(group2.name(), actualGroup.name());
    Assertions.assertEquals(
        Sets.newHashSet(group2.roleNames()), Sets.newHashSet(actualGroup.roleNames()));
  }

  @Test
  void insertGroup() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // insert group
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> groupMetaService.insertGroup(group1, false));
    Assertions.assertEquals(group1, groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));

    // insert duplicate group
    GroupEntity group1Exist =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo);
    Assertions.assertThrows(
        AlreadyExistsException.class, () -> groupMetaService.insertGroup(group1Exist, false));

    // insert overwrite
    GroupEntity group1Overwrite =
        createGroupEntity(
            group1.id(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1Overwrite",
            auditInfo);
    Assertions.assertDoesNotThrow(() -> groupMetaService.insertGroup(group1Overwrite, true));
    Assertions.assertEquals(
        "group1Overwrite",
        groupMetaService.getGroupByIdentifier(group1Overwrite.nameIdentifier()).name());
    Assertions.assertEquals(
        group1Overwrite, groupMetaService.getGroupByIdentifier(group1Overwrite.nameIdentifier()));

    // insert group with roles
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
    GroupEntity group2 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group2",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    Assertions.assertDoesNotThrow(() -> groupMetaService.insertGroup(group2, false));
    GroupEntity actualGroup = groupMetaService.getGroupByIdentifier(group2.nameIdentifier());
    Assertions.assertEquals(group2.name(), actualGroup.name());
    Assertions.assertEquals(
        Sets.newHashSet(group2.roleNames()), Sets.newHashSet(actualGroup.roleNames()));

    // insert duplicate group with roles
    GroupEntity group2Exist =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group2",
            auditInfo);
    Assertions.assertThrows(
        AlreadyExistsException.class, () -> groupMetaService.insertGroup(group2Exist, false));

    // insert overwrite group with roles
    GroupEntity group2Overwrite =
        createGroupEntity(
            group1.id(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group2Overwrite",
            auditInfo);
    Assertions.assertDoesNotThrow(() -> groupMetaService.insertGroup(group2Overwrite, true));
    Assertions.assertEquals(
        "group2Overwrite",
        groupMetaService.getGroupByIdentifier(group2Overwrite.nameIdentifier()).name());
    Assertions.assertEquals(
        group2Overwrite, groupMetaService.getGroupByIdentifier(group2Overwrite.nameIdentifier()));
  }

  @Test
  void deleteGroup() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    GroupMetaService groupMetaService = GroupMetaService.getInstance();
    RoleMetaService roleMetaService = RoleMetaService.getInstance();

    // delete group
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertDoesNotThrow(() -> groupMetaService.insertGroup(group1, false));
    Assertions.assertEquals(group1, groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertTrue(groupMetaService.deleteGroup(group1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));

    // delete group with roles
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
    GroupEntity group2 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group2",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    groupMetaService.insertGroup(group2, false);
    List<RolePO> rolePOs =
        SessionUtils.doWithCommitAndFetchResult(
            RoleMetaMapper.class, mapper -> mapper.listRolesByGroupId(group2.id()));
    Assertions.assertEquals(2, rolePOs.size());
    GroupEntity actualGroup = groupMetaService.getGroupByIdentifier(group2.nameIdentifier());
    Assertions.assertEquals(group2.name(), actualGroup.name());
    Assertions.assertEquals(
        Sets.newHashSet(group2.roleNames()), Sets.newHashSet(actualGroup.roleNames()));

    Assertions.assertTrue(groupMetaService.deleteGroup(group2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group2.nameIdentifier()));
    rolePOs =
        SessionUtils.doWithCommitAndFetchResult(
            RoleMetaMapper.class, mapper -> mapper.listRolesByGroupId(group2.id()));
    Assertions.assertEquals(0, rolePOs.size());
  }

  @Test
  void updateGroup() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    GroupMetaService groupMetaService = GroupMetaService.getInstance();
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
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    groupMetaService.insertGroup(group1, false);
    GroupEntity actualGroup = groupMetaService.getGroupByIdentifier(group1.nameIdentifier());
    Assertions.assertEquals(group1.name(), actualGroup.name());
    Assertions.assertEquals(
        Sets.newHashSet(group1.roleNames()), Sets.newHashSet(actualGroup.roleNames()));

    RoleEntity role3 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role3",
            auditInfo);
    roleMetaService.insertRole(role3, false);

    // update group (grant)
    Function<GroupEntity, GroupEntity> grantUpdater =
        group -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(group.auditInfo().creator())
                  .withCreateTime(group.auditInfo().createTime())
                  .withLastModifier("grantGroup")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(group.roleNames());
          List<Long> roleIds = Lists.newArrayList(group.roleIds());
          roleNames.add(role3.name());
          roleIds.add(role3.id());

          return GroupEntity.builder()
              .withNamespace(group.namespace())
              .withId(group.id())
              .withName(group.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };

    Assertions.assertNotNull(groupMetaService.updateGroup(group1.nameIdentifier(), grantUpdater));
    GroupEntity grantGroup =
        GroupMetaService.getInstance().getGroupByIdentifier(group1.nameIdentifier());
    Assertions.assertEquals(group1.id(), grantGroup.id());
    Assertions.assertEquals(group1.name(), grantGroup.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role2", "role3"), Sets.newHashSet(grantGroup.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role2.id(), role3.id()), Sets.newHashSet(grantGroup.roleIds()));
    Assertions.assertEquals("creator", grantGroup.auditInfo().creator());
    Assertions.assertEquals("grantGroup", grantGroup.auditInfo().lastModifier());

    // update group (revoke)
    Function<GroupEntity, GroupEntity> revokeUpdater =
        group -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(group.auditInfo().creator())
                  .withCreateTime(group.auditInfo().createTime())
                  .withLastModifier("revokeGroup")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(group.roleNames());
          List<Long> roleIds = Lists.newArrayList(group.roleIds());
          roleIds.remove(roleNames.indexOf("role2"));
          roleNames.remove("role2");

          return GroupEntity.builder()
              .withNamespace(group.namespace())
              .withId(group.id())
              .withName(group.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };

    Assertions.assertNotNull(groupMetaService.updateGroup(group1.nameIdentifier(), revokeUpdater));
    GroupEntity revokeGroup =
        GroupMetaService.getInstance().getGroupByIdentifier(group1.nameIdentifier());
    Assertions.assertEquals(group1.id(), revokeGroup.id());
    Assertions.assertEquals(group1.name(), revokeGroup.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role3"), Sets.newHashSet(revokeGroup.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role3.id()), Sets.newHashSet(revokeGroup.roleIds()));
    Assertions.assertEquals("creator", revokeGroup.auditInfo().creator());
    Assertions.assertEquals("revokeGroup", revokeGroup.auditInfo().lastModifier());

    RoleEntity role4 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role4",
            auditInfo);
    roleMetaService.insertRole(role4, false);

    // update group (grant & revoke)
    Function<GroupEntity, GroupEntity> grantRevokeUpdater =
        group -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(group.auditInfo().creator())
                  .withCreateTime(group.auditInfo().createTime())
                  .withLastModifier("grantRevokeUser")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(group.roleNames());
          List<Long> roleIds = Lists.newArrayList(group.roleIds());
          roleIds.remove(roleNames.indexOf("role3"));
          roleNames.remove("role3");
          roleIds.add(role4.id());
          roleNames.add(role4.name());

          return GroupEntity.builder()
              .withNamespace(group.namespace())
              .withId(group.id())
              .withName(group.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };
    Assertions.assertNotNull(
        groupMetaService.updateGroup(group1.nameIdentifier(), grantRevokeUpdater));
    GroupEntity grantRevokeGroup =
        GroupMetaService.getInstance().getGroupByIdentifier(group1.nameIdentifier());
    Assertions.assertEquals(group1.id(), grantRevokeGroup.id());
    Assertions.assertEquals(group1.name(), grantRevokeGroup.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role4"), Sets.newHashSet(grantRevokeGroup.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role4.id()), Sets.newHashSet(grantRevokeGroup.roleIds()));
    Assertions.assertEquals("creator", grantRevokeGroup.auditInfo().creator());
    Assertions.assertEquals("grantRevokeUser", grantRevokeGroup.auditInfo().lastModifier());

    // no update
    Function<GroupEntity, GroupEntity> noUpdater =
        group -> {
          AuditInfo updateAuditInfo =
              AuditInfo.builder()
                  .withCreator(group.auditInfo().creator())
                  .withCreateTime(group.auditInfo().createTime())
                  .withLastModifier("noUpdateUser")
                  .withLastModifiedTime(Instant.now())
                  .build();

          List<String> roleNames = Lists.newArrayList(group.roleNames());
          List<Long> roleIds = Lists.newArrayList(group.roleIds());

          return GroupEntity.builder()
              .withNamespace(group.namespace())
              .withId(group.id())
              .withName(group.name())
              .withRoleNames(roleNames)
              .withRoleIds(roleIds)
              .withAuditInfo(updateAuditInfo)
              .build();
        };
    Assertions.assertNotNull(groupMetaService.updateGroup(group1.nameIdentifier(), noUpdater));
    GroupEntity noUpdaterGroup =
        GroupMetaService.getInstance().getGroupByIdentifier(group1.nameIdentifier());
    Assertions.assertEquals(group1.id(), noUpdaterGroup.id());
    Assertions.assertEquals(group1.name(), noUpdaterGroup.name());
    Assertions.assertEquals(
        Sets.newHashSet("role1", "role4"), Sets.newHashSet(noUpdaterGroup.roleNames()));
    Assertions.assertEquals(
        Sets.newHashSet(role1.id(), role4.id()), Sets.newHashSet(noUpdaterGroup.roleIds()));
    Assertions.assertEquals("creator", noUpdaterGroup.auditInfo().creator());
    Assertions.assertEquals("grantRevokeUser", noUpdaterGroup.auditInfo().lastModifier());
  }

  @Test
  void deleteMetalake() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    GroupMetaService groupMetaService = GroupMetaService.getInstance();
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
    RoleEntity role3 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role3",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    roleMetaService.insertRole(role3, false);
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    GroupEntity group2 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group2",
            auditInfo,
            Lists.newArrayList(role3.name()),
            Lists.newArrayList(role3.id()));
    groupMetaService.insertGroup(group1, false);
    groupMetaService.insertGroup(group2, false);

    Assertions.assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertEquals(2, roleMetaService.listRolesByGroupId(group1.id()).size());
    Assertions.assertEquals(
        group2.name(), groupMetaService.getGroupByIdentifier(group2.nameIdentifier()).name());
    Assertions.assertEquals(1, roleMetaService.listRolesByGroupId(group2.id()).size());

    // delete metalake without cascade
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group2.nameIdentifier()));
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group1.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group2.id()).size());
  }

  @Test
  void deleteMetalakeCascade() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    GroupMetaService groupMetaService = GroupMetaService.getInstance();
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
    RoleEntity role3 =
        createRoleEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofRoleNamespace(metalakeName),
            "role3",
            auditInfo);
    roleMetaService.insertRole(role1, false);
    roleMetaService.insertRole(role2, false);
    roleMetaService.insertRole(role3, false);
    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    GroupEntity group2 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofGroupNamespace(metalakeName),
            "group2",
            auditInfo,
            Lists.newArrayList(role3.name()),
            Lists.newArrayList(role3.id()));
    groupMetaService.insertGroup(group1, false);
    groupMetaService.insertGroup(group2, false);

    Assertions.assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertEquals(2, roleMetaService.listRolesByGroupId(group1.id()).size());
    Assertions.assertEquals(
        group2.name(), groupMetaService.getGroupByIdentifier(group2.nameIdentifier()).name());
    Assertions.assertEquals(1, roleMetaService.listRolesByGroupId(group2.id()).size());

    // delete metalake with cascade
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), true));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group2.nameIdentifier()));
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group1.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group2.id()).size());
  }

  @Test
  void deleteGroupMetasByLegacyTimeLine() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    GroupMetaService groupMetaService = GroupMetaService.getInstance();
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

    GroupEntity group1 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "group1",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    GroupEntity group2 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "group2",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    GroupEntity group3 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "group3",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    GroupEntity group4 =
        createGroupEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            AuthorizationUtils.ofUserNamespace(metalakeName),
            "group4",
            auditInfo,
            Lists.newArrayList(role1.name(), role2.name()),
            Lists.newArrayList(role1.id(), role2.id()));
    groupMetaService.insertGroup(group1, false);
    groupMetaService.insertGroup(group2, false);
    groupMetaService.insertGroup(group3, false);
    groupMetaService.insertGroup(group4, false);

    // hard delete before soft delete
    int deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 4);
    Assertions.assertEquals(0, deletedCount);
    Assertions.assertEquals(
        group1.name(), groupMetaService.getGroupByIdentifier(group1.nameIdentifier()).name());
    Assertions.assertEquals(
        group2.name(), groupMetaService.getGroupByIdentifier(group2.nameIdentifier()).name());
    Assertions.assertEquals(
        group3.name(), groupMetaService.getGroupByIdentifier(group3.nameIdentifier()).name());
    Assertions.assertEquals(
        group4.name(), groupMetaService.getGroupByIdentifier(group4.nameIdentifier()).name());
    Assertions.assertEquals(2, roleMetaService.listRolesByGroupId(group1.id()).size());
    Assertions.assertEquals(2, roleMetaService.listRolesByGroupId(group2.id()).size());
    Assertions.assertEquals(2, roleMetaService.listRolesByGroupId(group3.id()).size());
    Assertions.assertEquals(2, roleMetaService.listRolesByGroupId(group4.id()).size());
    Assertions.assertEquals(4, countGroups(metalake.id()));
    Assertions.assertEquals(8, countGroupRoleRels());

    // delete metalake
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), true));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group1.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group2.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group3.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> groupMetaService.getGroupByIdentifier(group4.nameIdentifier()));
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group1.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group2.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group3.id()).size());
    Assertions.assertEquals(0, roleMetaService.listRolesByGroupId(group4.id()).size());
    Assertions.assertEquals(4, countGroups(metalake.id()));
    Assertions.assertEquals(8, countGroupRoleRels());

    // hard delete after soft delete
    deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 3);
    Assertions.assertEquals(6, deletedCount); // delete 3 group + 3 groupRoleRel
    Assertions.assertEquals(1, countGroups(metalake.id())); // 4 - 3
    Assertions.assertEquals(5, countGroupRoleRels()); // 8 - 3

    deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 3);
    Assertions.assertEquals(4, deletedCount); // delete 1 group + 3 groupRoleRel
    Assertions.assertEquals(0, countGroups(metalake.id()));
    Assertions.assertEquals(2, countGroupRoleRels()); // 5 - 3

    deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 3);
    Assertions.assertEquals(2, deletedCount);
    Assertions.assertEquals(0, countGroups(metalake.id()));
    Assertions.assertEquals(0, countGroupRoleRels());

    deletedCount =
        groupMetaService.deleteGroupMetasByLegacyTimeLine(Instant.now().toEpochMilli() + 1000, 3);
    Assertions.assertEquals(0, deletedCount); // no more to delete
  }

  private Integer countGroups(Long metalakeId) {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM group_meta WHERE metalake_id = %d", metalakeId))) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }

  private Integer countGroupRoleRels() {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM group_role_rel")) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }
}
