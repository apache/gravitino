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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
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
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.server.authorization.GravitinoAuthorizer;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.model.Model;

/** The Jcasbin implementation of GravitinoAuthorizer. */
public class JcasbinAuthorizer implements GravitinoAuthorizer {

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

  private boolean authorizeByJcasbin(Long userId, MetadataObject metadataObject, String privilege) {
    Long metadataId = MetadataIdConverter.doConvert(metadataObject);
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
    loadPrivilege(metalake, username, userId);
    return authorizeByJcasbin(userId, metadataObject, privilege);
  }

  private void loadPrivilege(String metalake, String username, Long userId) {
    try {
      EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
      List<RoleEntity> entities =
          entityStore
              .relationOperations()
              .listEntitiesByRelation(
                  SupportsRelationOperations.Type.ROLE_USER_REL,
                  NameIdentifierUtil.ofUser(metalake, username),
                  Entity.EntityType.ROLE);

      for (RoleEntity role : entities) {
        Long roleId = role.id();
        if (loadedRoles.contains(roleId)) {
          continue;
        }
        enforcer.addRoleForUser(String.valueOf(userId), String.valueOf(roleId));
        loadPolicyByRoleId(roleId);
        loadedRoles.add(roleId);
      }
      // TODO load owner relationship
    } catch (Exception e) {
      throw new RuntimeException("Can not load privilege", e);
    }
  }

  private void loadPolicyByRoleId(Long roleId) {
    try {
      List<SecurableObjectPO> securableObjects =
          RoleMetaService.listSecurableObjectsByRoleId(roleId);
      for (SecurableObjectPO securableObject : securableObjects) {
        String privilegeNamesString = securableObject.getPrivilegeNames();
        String privilegeConditionsString = securableObject.getPrivilegeConditions();
        ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
        List<String> privileges =
            objectMapper.readValue(privilegeNamesString, new TypeReference<List<String>>() {});
        List<String> privilegeConditions =
            objectMapper.readValue(privilegeConditionsString, new TypeReference<List<String>>() {});
        for (int i = 0; i < privileges.size(); i++) {
          enforcer.addPolicy(
              String.valueOf(securableObject.getRoleId()),
              securableObject.getType(),
              String.valueOf(securableObject.getMetadataObjectId()),
              privileges.get(i).toUpperCase(),
              privilegeConditions.get(i).toLowerCase());
        }
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Can not parse privilege names", e);
    }
  }
}
