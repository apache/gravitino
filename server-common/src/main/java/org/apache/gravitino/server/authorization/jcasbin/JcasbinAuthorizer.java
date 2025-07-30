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

package org.apache.gravitino.server.authorization.jcasbin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.IOUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.server.authorization.GravitinoAuthorizer;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Jcasbin implementation of GravitinoAuthorizer. */
public class JcasbinAuthorizer implements GravitinoAuthorizer {

  private static final Logger LOG = LoggerFactory.getLogger(JcasbinAuthorizer.class);

  /** Jcasbin enforcer is used for metadata authorization. */
  private Enforcer enforcer;

  /**
   * loadedRoles is used to cache roles that have loaded permissions. When the permissions of a role
   * are updated, they should be removed from it.
   */
  private Set<Long> loadedRoles = ConcurrentHashMap.newKeySet();

  @Override
  public void initialize() {
    try (InputStream modelStream =
        JcasbinAuthorizer.class.getResourceAsStream("/jcasbin_model.conf")) {
      Preconditions.checkNotNull(modelStream, "Jcasbin model file can not found.");
      String modelData = IOUtils.toString(modelStream, StandardCharsets.UTF_8);
      Model model = new Model();
      model.loadModelFromText(modelData);
      enforcer = new Enforcer(model, new GravitinoAdapter());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean authorize(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege) {
    return authorizeInternal(principal, metalake, metadataObject, privilege.name());
  }

  @Override
  public boolean isOwner(Principal principal, String metalake, MetadataObject metadataObject) {
    return authorizeInternal(principal, metalake, metadataObject, AuthConstants.OWNER);
  }

  @Override
  public void handleRolePrivilegeChange(Long roleId) {
    loadedRoles.remove(roleId);
    enforcer.deleteRole(String.valueOf(roleId));
  }

  @Override
  public void handleMetadataOwnerChange(
      String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(nameIdentifier, type);
    Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
    ImmutableList<String> policy =
        ImmutableList.of(
            String.valueOf(oldOwnerId),
            String.valueOf(metadataObject.type()),
            String.valueOf(metadataId),
            AuthConstants.OWNER,
            "allow");
    enforcer.removePolicy(policy);
  }

  @Override
  public void close() throws IOException {}

  private boolean authorizeInternal(
      Principal principal, String metalake, MetadataObject metadataObject, String privilege) {
    String username = principal.getName();
    return loadPrivilegeAndAuthorize(username, metalake, metadataObject, privilege);
  }

  private static Long getMetalakeId(String metalake) {
    try {
      EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
      BaseMetalake metalakeEntity =
          entityStore.get(
              NameIdentifierUtil.ofMetalake(metalake),
              Entity.EntityType.METALAKE,
              BaseMetalake.class);
      Long metalakeId = metalakeEntity.id();
      return metalakeId;
    } catch (Exception e) {
      throw new RuntimeException("Can not get metalake id by entity store", e);
    }
  }

  private boolean authorizeByJcasbin(
      Long userId, MetadataObject metadataObject, Long metadataId, String privilege) {
    return enforcer.enforce(
        String.valueOf(userId),
        String.valueOf(metadataObject.type()),
        String.valueOf(metadataId),
        privilege);
  }

  private boolean loadPrivilegeAndAuthorize(
      String username, String metalake, MetadataObject metadataObject, String privilege) {
    Long metalakeId = getMetalakeId(metalake);
    Long userId = UserMetaService.getInstance().getUserIdByMetalakeIdAndName(metalakeId, username);
    Long metadataId = MetadataIdConverter.getID(metadataObject, metalake);
    loadPrivilege(metalake, username, userId, metadataObject, metadataId);
    return authorizeByJcasbin(userId, metadataObject, metadataId, privilege);
  }

  private void loadPrivilege(
      String metalake,
      String username,
      Long userId,
      MetadataObject metadataObject,
      Long metadataObjectId) {
    try {
      EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
      NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(metalake, username);
      List<RoleEntity> entities =
          entityStore
              .relationOperations()
              .listEntitiesByRelation(
                  SupportsRelationOperations.Type.ROLE_USER_REL,
                  userNameIdentifier,
                  Entity.EntityType.USER);

      for (RoleEntity role : entities) {
        Long roleId = role.id();
        if (loadedRoles.contains(roleId)) {
          continue;
        }
        enforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
        loadPolicyByRoleEntity(role);
        loadedRoles.add(roleId);
      }
      loadOwnerPolicy(metalake, metadataObject, metadataObjectId);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private void loadOwnerPolicy(String metalake, MetadataObject metadataObject, Long metadataId) {
    try {
      NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
      EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
      List<? extends Entity> owners =
          entityStore
              .relationOperations()
              .listEntitiesByRelation(
                  SupportsRelationOperations.Type.OWNER_REL,
                  entityIdent,
                  Entity.EntityType.valueOf(metadataObject.type().name()));
      for (Entity ownerEntity : owners) {
        if (ownerEntity instanceof UserEntity) {
          UserEntity user = (UserEntity) ownerEntity;
          ImmutableList<String> policy =
              ImmutableList.of(
                  String.valueOf(user.id()),
                  String.valueOf(metadataObject.type()),
                  String.valueOf(metadataId),
                  AuthConstants.OWNER,
                  "allow");
          enforcer.addPolicy(policy);
        }
      }
    } catch (IOException e) {
      LOG.warn("Can not load metadata owner", e);
    }
  }

  private void loadPolicyByRoleEntity(RoleEntity roleEntity) {
    String metalake = NameIdentifierUtil.getMetalake(roleEntity.nameIdentifier());
    List<SecurableObject> securableObjects = roleEntity.securableObjects();

    for (SecurableObject securableObject : securableObjects) {
      for (Privilege privilege : securableObject.privileges()) {
        Privilege.Condition condition = privilege.condition();
        enforcer.addPolicy(
            String.valueOf(roleEntity.id()),
            securableObject.type().name(),
            String.valueOf(MetadataIdConverter.getID(securableObject, metalake)),
            privilege.name().name().toUpperCase(),
            condition.name().toLowerCase());
      }
    }
  }
}
