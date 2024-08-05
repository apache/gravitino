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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The service class for role metadata. It provides the basic database operations for role. */
public class RoleMetaService {

  private static final Logger LOG = LoggerFactory.getLogger(RoleMetaService.class);
  private static final RoleMetaService INSTANCE = new RoleMetaService();

  public static RoleMetaService getInstance() {
    return INSTANCE;
  }

  private RoleMetaService() {}

  private RolePO getRolePOByMetalakeIdAndName(Long metalakeId, String roleName) {
    RolePO rolePO =
        SessionUtils.getWithoutCommit(
            RoleMetaMapper.class,
            mapper -> mapper.selectRoleMetaByMetalakeIdAndName(metalakeId, roleName));

    if (rolePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.ROLE.name().toLowerCase(),
          roleName);
    }
    return rolePO;
  }

  public Long getRoleIdByMetalakeIdAndName(Long metalakeId, String roleName) {
    Long roleId =
        SessionUtils.getWithoutCommit(
            RoleMetaMapper.class,
            mapper -> mapper.selectRoleIdByMetalakeIdAndName(metalakeId, roleName));

    if (roleId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.ROLE.name().toLowerCase(),
          roleName);
    }
    return roleId;
  }

  public List<RolePO> listRolesByUserId(Long userId) {
    return SessionUtils.getWithoutCommit(
        RoleMetaMapper.class, mapper -> mapper.listRolesByUserId(userId));
  }

  public List<RolePO> listRolesByGroupId(Long groupId) {
    return SessionUtils.getWithoutCommit(
        RoleMetaMapper.class, mapper -> mapper.listRolesByGroupId(groupId));
  }

  public void insertRole(RoleEntity roleEntity, boolean overwritten) throws IOException {
    try {
      AuthorizationUtils.checkRole(roleEntity.nameIdentifier());

      Long metalakeId =
          MetalakeMetaService.getInstance().getMetalakeIdByName(roleEntity.namespace().level(0));
      RolePO.Builder builder = RolePO.builder().withMetalakeId(metalakeId);
      RolePO rolePO = POConverters.initializeRolePOWithVersion(roleEntity, builder);
      List<SecurableObjectPO> securableObjectPOs = Lists.newArrayList();
      for (SecurableObject object : roleEntity.securableObjects()) {
        SecurableObjectPO.Builder objectBuilder =
            POConverters.initializeSecurablePOBuilderWithVersion(
                roleEntity.id(), object, getEntityType(object));
        objectBuilder.withEntityId(
            MetadataObjectService.getMetadataObjectId(
                metalakeId, object.fullName(), object.type()));
        securableObjectPOs.add(objectBuilder.build());
      }

      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  SecurableObjectMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.softDeleteSecurableObjectsByRoleId(rolePO.getRoleId());
                    }
                    if (!securableObjectPOs.isEmpty()) {
                      mapper.batchInsertSecurableObjects(securableObjectPOs);
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  RoleMetaMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.insertRoleMetaOnDuplicateKeyUpdate(rolePO);
                    } else {
                      mapper.insertRoleMeta(rolePO);
                    }
                  }));

    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.ROLE, roleEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public RoleEntity getRoleByIdentifier(NameIdentifier identifier) {
    AuthorizationUtils.checkRole(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    RolePO rolePO = getRolePOByMetalakeIdAndName(metalakeId, identifier.name());

    List<SecurableObjectPO> securableObjectPOs = listSecurableObjectsByRoleId(rolePO.getRoleId());
    List<SecurableObject> securableObjects = Lists.newArrayList();

    for (SecurableObjectPO securableObjectPO : securableObjectPOs) {
      String fullName =
          MetadataObjectService.getMetadataObjectFullName(
              securableObjectPO.getType(), securableObjectPO.getEntityId());
      if (fullName != null) {
        securableObjects.add(
            POConverters.fromSecurableObjectPO(
                fullName, securableObjectPO, getType(securableObjectPO.getType())));
      } else {
        LOG.info(
            "The securable object {} {} may be deleted",
            securableObjectPO.getEntityId(),
            securableObjectPO.getType());
      }
    }

    return POConverters.fromRolePO(rolePO, securableObjects, identifier.namespace());
  }

  public boolean deleteRole(NameIdentifier identifier) {
    AuthorizationUtils.checkRole(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    Long roleId = getRoleIdByMetalakeIdAndName(metalakeId, identifier.name());

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                RoleMetaMapper.class, mapper -> mapper.softDeleteRoleMetaByRoleId(roleId)),
        () ->
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class, mapper -> mapper.softDeleteUserRoleRelByRoleId(roleId)),
        () ->
            SessionUtils.doWithoutCommit(
                GroupRoleRelMapper.class, mapper -> mapper.softDeleteGroupRoleRelByRoleId(roleId)),
        () ->
            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper -> mapper.softDeleteSecurableObjectsByRoleId(roleId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        roleId, MetadataObject.Type.ROLE.name())));
    return true;
  }

  private List<SecurableObjectPO> listSecurableObjectsByRoleId(Long roleId) {
    return SessionUtils.getWithoutCommit(
        SecurableObjectMapper.class, mapper -> mapper.listSecurableObjectsByRoleId(roleId));
  }

  public int deleteRoleMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] roleDeletedCount = new int[] {0};
    int[] userRoleRelDeletedCount = new int[] {0};
    int[] groupRoleRelDeletedCount = new int[] {0};
    int[] securableObjectsCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            roleDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    RoleMetaMapper.class,
                    mapper -> mapper.deleteRoleMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            userRoleRelDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    UserRoleRelMapper.class,
                    mapper -> mapper.deleteUserRoleRelMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            groupRoleRelDeletedCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    GroupRoleRelMapper.class,
                    mapper ->
                        mapper.deleteGroupRoleRelMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            securableObjectsCount[0] =
                SessionUtils.doWithoutCommitAndFetchResult(
                    SecurableObjectMapper.class,
                    mapper ->
                        mapper.deleteSecurableObjectsByLegacyTimeline(legacyTimeline, limit)));

    return roleDeletedCount[0]
        + userRoleRelDeletedCount[0]
        + groupRoleRelDeletedCount[0]
        + securableObjectsCount[0];
  }

  private MetadataObject.Type getType(String type) {
    return MetadataObject.Type.valueOf(type);
  }

  private String getEntityType(SecurableObject securableObject) {
    return securableObject.type().name();
  }
}
