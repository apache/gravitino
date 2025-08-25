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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
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
import org.apache.gravitino.utils.NameIdentifierUtil;
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

  public List<RoleEntity> listRolesByUserIdent(NameIdentifier userIdent) {
    UserEntity user = UserMetaService.getInstance().getUserByIdentifier(userIdent);
    String metalake = NameIdentifierUtil.getMetalake(userIdent);
    List<RolePO> rolePOs = listRolesByUserId(user.id());
    return rolePOs.stream()
        .map(
            po ->
                POConverters.fromRolePO(
                    po, Collections.emptyList(), AuthorizationUtils.ofRoleNamespace(metalake)))
        .collect(Collectors.toList());
  }

  public List<RoleEntity> listRolesByMetadataObject(
      NameIdentifier metadataObjectIdent, Entity.EntityType metadataObjectType, boolean allFields) {
    String metalake = NameIdentifierUtil.getMetalake(metadataObjectIdent);
    long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(metadataObjectIdent, metadataObjectType);
    long metadataObjectId =
        MetadataObjectService.getMetadataObjectId(
            metalakeId, metadataObject.fullName(), metadataObject.type());
    List<RolePO> rolePOs =
        SessionUtils.getWithoutCommit(
            RoleMetaMapper.class,
            mapper ->
                mapper.listRolesByMetadataObjectIdAndType(
                    metadataObjectId, metadataObject.type().name()));
    return rolePOs.stream()
        .map(
            po -> {
              if (allFields) {
                return POConverters.fromRolePO(
                    po, listSecurableObjects(po), AuthorizationUtils.ofRoleNamespace(metalake));
              } else {
                return POConverters.fromRolePO(
                    po, Collections.emptyList(), AuthorizationUtils.ofRoleNamespace(metalake));
              }
            })
        .collect(Collectors.toList());
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
        objectBuilder.withMetadataObjectId(
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

  public <E extends Entity & HasIdentifier> RoleEntity updateRole(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    AuthorizationUtils.checkRole(identifier);

    try {
      String metalake = identifier.namespace().level(0);
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);

      RolePO rolePO = getRolePOByMetalakeIdAndName(metalakeId, identifier.name());
      RoleEntity oldRoleEntity =
          POConverters.fromRolePO(
              rolePO, listSecurableObjects(rolePO), AuthorizationUtils.ofRoleNamespace(metalake));

      RoleEntity newRoleEntity = (RoleEntity) updater.apply((E) oldRoleEntity);

      Preconditions.checkArgument(
          Objects.equals(oldRoleEntity.id(), newRoleEntity.id()),
          "The updated role entity id: %s should be same with the role entity id before: %s",
          newRoleEntity.id(),
          oldRoleEntity.id());

      Set<SecurableObject> oldObjects = new HashSet<>(oldRoleEntity.securableObjects());
      Set<SecurableObject> newObjects = new HashSet<>(newRoleEntity.securableObjects());
      Set<SecurableObject> insertObjects = Sets.difference(newObjects, oldObjects);
      Set<SecurableObject> deleteObjects = Sets.difference(oldObjects, newObjects);

      if (insertObjects.isEmpty() && deleteObjects.isEmpty()) {
        return newRoleEntity;
      }

      List<SecurableObjectPO> deleteSecurableObjectPOs =
          toSecurableObjectPOs(deleteObjects, oldRoleEntity, metalakeId);

      List<SecurableObjectPO> insertSecurableObjectPOs =
          toSecurableObjectPOs(insertObjects, oldRoleEntity, metalakeId);

      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  RoleMetaMapper.class,
                  mapper ->
                      mapper.updateRoleMeta(
                          POConverters.updateRolePOWithVersion(rolePO, newRoleEntity), rolePO)),
          () -> {
            if (deleteSecurableObjectPOs.isEmpty()) {
              return;
            }

            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper -> mapper.batchSoftDeleteSecurableObjects(deleteSecurableObjectPOs));
          },
          () -> {
            if (insertSecurableObjectPOs.isEmpty()) {
              return;
            }

            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper -> mapper.batchInsertSecurableObjects(insertSecurableObjectPOs));
          });

      return newRoleEntity;
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(re, Entity.EntityType.ROLE, identifier.toString());
      throw re;
    }
  }

  private List<SecurableObjectPO> toSecurableObjectPOs(
      Set<SecurableObject> deleteObjects, RoleEntity oldRoleEntity, Long metalakeId) {
    List<SecurableObjectPO> securableObjectPOs = Lists.newArrayList();
    for (SecurableObject object : deleteObjects) {
      SecurableObjectPO.Builder objectBuilder =
          POConverters.initializeSecurablePOBuilderWithVersion(
              oldRoleEntity.id(), object, getEntityType(object));
      objectBuilder.withMetadataObjectId(
          MetadataObjectService.getMetadataObjectId(metalakeId, object.fullName(), object.type()));
      securableObjectPOs.add(objectBuilder.build());
    }
    return securableObjectPOs;
  }

  public RoleEntity getRoleByIdentifier(NameIdentifier identifier) {
    AuthorizationUtils.checkRole(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    RolePO rolePO = getRolePOByMetalakeIdAndName(metalakeId, identifier.name());

    List<SecurableObject> securableObjects = listSecurableObjects(rolePO);

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

  public static List<SecurableObjectPO> listSecurableObjectsByRoleId(Long roleId) {
    return SessionUtils.getWithoutCommit(
        SecurableObjectMapper.class, mapper -> mapper.listSecurableObjectsByRoleId(roleId));
  }

  public List<RoleEntity> listRolesByNamespace(Namespace namespace) {
    AuthorizationUtils.checkRoleNamespace(namespace);
    String metalakeName = namespace.level(0);

    List<RolePO> rolePOs =
        SessionUtils.getWithoutCommit(
            RoleMetaMapper.class, mapper -> mapper.listRolePOsByMetalake(metalakeName));

    return rolePOs.stream()
        .map(
            po ->
                POConverters.fromRolePO(
                    po, Collections.emptyList(), AuthorizationUtils.ofRoleNamespace(metalakeName)))
        .collect(Collectors.toList());
  }

  public int deleteRoleMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] roleDeletedCount = new int[] {0};
    int[] userRoleRelDeletedCount = new int[] {0};
    int[] groupRoleRelDeletedCount = new int[] {0};
    int[] securableObjectsCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            roleDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    RoleMetaMapper.class,
                    mapper -> mapper.deleteRoleMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            userRoleRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    UserRoleRelMapper.class,
                    mapper -> mapper.deleteUserRoleRelMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            groupRoleRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    GroupRoleRelMapper.class,
                    mapper ->
                        mapper.deleteGroupRoleRelMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            securableObjectsCount[0] =
                SessionUtils.getWithoutCommit(
                    SecurableObjectMapper.class,
                    mapper ->
                        mapper.deleteSecurableObjectsByLegacyTimeline(legacyTimeline, limit)));

    return roleDeletedCount[0]
        + userRoleRelDeletedCount[0]
        + groupRoleRelDeletedCount[0]
        + securableObjectsCount[0];
  }

  private static List<SecurableObject> listSecurableObjects(RolePO po) {
    List<SecurableObjectPO> securableObjectPOs = listSecurableObjectsByRoleId(po.getRoleId());
    List<SecurableObject> securableObjects = Lists.newArrayList();

    securableObjectPOs.stream()
        .collect(Collectors.groupingBy(SecurableObjectPO::getType))
        .forEach(
            (type, objects) -> {
              List<Long> objectIds =
                  objects.stream()
                      .map(SecurableObjectPO::getMetadataObjectId)
                      .collect(Collectors.toList());

              // dynamically calling getter function based on type
              Map<Long, String> objectIdAndNameMap =
                  Optional.of(MetadataObject.Type.valueOf(type))
                      .map(MetadataObjectService.TYPE_TO_FULLNAME_FUNCTION_MAP::get)
                      .map(getter -> getter.apply(objectIds))
                      .orElseThrow(
                          () ->
                              // for example: MetadataObject.Type.COLUMN
                              new IllegalArgumentException(
                                  "Unsupported metadata object type: " + type));

              for (SecurableObjectPO securableObjectPO : objects) {
                String fullName = objectIdAndNameMap.get(securableObjectPO.getMetadataObjectId());
                if (fullName != null) {
                  securableObjects.add(
                      POConverters.fromSecurableObjectPO(
                          fullName, securableObjectPO, getType(securableObjectPO.getType())));
                } else {
                  LOG.warn(
                      "The securable object {} {} may be deleted",
                      securableObjectPO.getMetadataObjectId(),
                      securableObjectPO.getType());
                }
              }
            });
    return securableObjects;
  }

  private static RolePO getRolePOByMetalakeIdAndName(Long metalakeId, String roleName) {
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

  private static MetadataObject.Type getType(String type) {
    return MetadataObject.Type.valueOf(type);
  }

  private static String getEntityType(SecurableObject securableObject) {
    return securableObject.type().name();
  }
}
