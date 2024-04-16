/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.meta.RoleEntity;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* The utilization class of authorization module*/
class AuthorizationUtils {

  static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in th metalake %s";
  static final String GROUP_DOES_NOT_EXIST_MSG = "Group %s does not exist in th metalake %s";
  static final String ROLE_DOES_NOT_EXIST_MSG = "Role %s does not exist in th metalake %s";
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationUtils.class);
  private static final String METALAKE_DOES_NOT_EXIST_MSG = "Metalake %s does not exist";

  private AuthorizationUtils() {}

  static void checkMetalakeExists(String metalake) throws NoSuchMetalakeException {
    try {
      EntityStore store = GravitinoEnv.getInstance().entityStore();

      NameIdentifier metalakeIdent = NameIdentifier.ofMetalake(metalake);
      if (!store.exists(metalakeIdent, Entity.EntityType.METALAKE)) {
        LOG.warn("Metalake {} does not exist", metalakeIdent);
        throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalakeIdent);
      }
    } catch (IOException e) {
      LOG.error("Failed to do storage operation", e);
      throw new RuntimeException(e);
    }
  }

  static RoleEntity getRoleEntity(NameIdentifier identifier) {

    Cache<NameIdentifier, RoleEntity> cache =
        GravitinoEnv.getInstance().accessControlManager().getRoleCache();
    EntityStore store = GravitinoEnv.getInstance().entityStore();

    return cache.get(
        identifier,
        id -> {
          try {
            return store.get(identifier, Entity.EntityType.ROLE, RoleEntity.class);
          } catch (IOException ioe) {
            LOG.error("getting roles {} failed  due to storage issues", identifier, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  static List<RoleEntity> getValidRoles(
      String metalake, List<String> roleNames, List<Long> roleIds) {
    List<RoleEntity> roleEntities = Lists.newArrayList();
    if (roleNames == null || roleNames.isEmpty()) {
      return roleEntities;
    }

    int index = 0;
    for (String role : roleNames) {
      try {

        RoleEntity roleEntity =
            AuthorizationUtils.getRoleEntity(NameIdentifierUtils.ofRole(metalake, role));

        if (roleEntity.id().equals(roleIds.get(index))) {
          roleEntities.add(roleEntity);
        }
        index++;

      } catch (NoSuchEntityException nse) {
        // ignore this entity
      }
    }
    return roleEntities;
  }
}
