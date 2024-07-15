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
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There are two kinds of admin roles in the system: service admin and metalake admin. The service
 * admin is configured instead of managing by APIs. It is responsible for creating metalake admin.
 * If Gravitino enables authorization, service admin is required. Metalake admin can create a
 * metalake or drops its metalake. The metalake admin will be responsible for managing the access
 * control. AdminManager operates underlying store using the lock because kv storage needs the lock.
 */
class AdminManager {

  private static final Logger LOG = LoggerFactory.getLogger(AdminManager.class);

  private final EntityStore store;
  private final IdGenerator idGenerator;
  private final List<String> serviceAdmins;

  AdminManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.serviceAdmins = config.get(Configs.SERVICE_ADMINS);
  }

  User addMetalakeAdmin(String user) {

    UserEntity userEntity =
        UserEntity.builder()
            .withId(idGenerator.nextId())
            .withName(user)
            .withNamespace(
                Namespace.of(
                    Entity.SYSTEM_METALAKE_RESERVED_NAME,
                    Entity.AUTHORIZATION_CATALOG_NAME,
                    Entity.ADMIN_SCHEMA_NAME))
            .withRoleNames(Lists.newArrayList())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(userEntity, false /* overwritten */);
      return userEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake admin already exists", user, e);
      throw new UserAlreadyExistsException("User %s in the metalake admin already exists", user);
    } catch (IOException ioe) {
      LOG.error("Adding user {} failed to the metalake admin due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean removeMetalakeAdmin(String user) {
    try {
      return store.delete(ofMetalakeAdmin(user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} from the metalake admin {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean isServiceAdmin(String user) {
    return serviceAdmins.contains(user);
  }

  boolean isMetalakeAdmin(String user) {
    try {
      return store.exists(ofMetalakeAdmin(user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Fail to check whether {} is the metalake admin {} due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private NameIdentifier ofMetalakeAdmin(String user) {
    return NameIdentifier.of(
        Entity.SYSTEM_METALAKE_RESERVED_NAME,
        Entity.AUTHORIZATION_CATALOG_NAME,
        Entity.ADMIN_SCHEMA_NAME,
        user);
  }
}
