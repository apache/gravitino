/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.ForbiddenException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* The utilization class of authorization module*/
public class AuthorizationUtils {

  static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in th metalake %s";
  static final String GROUP_DOES_NOT_EXIST_MSG = "Group %s does not exist in th metalake %s";
  static final String ROLE_DOES_NOT_EXIST_MSG = "Role %s does not exist in th metalake %s";
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationUtils.class);
  private static final String METALAKE_DOES_NOT_EXIST_MSG = "Metalake %s does not exist";

  private AuthorizationUtils() {}

  public static NameIdentifier ofRole(String metalake, String role) {
    return NameIdentifier.of(
        metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME, role);
  }

  public static NameIdentifier ofGroup(String metalake, String group) {
    return NameIdentifier.of(
        metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME, group);
  }

  public static NameIdentifier ofUser(String metalake, String user) {
    return NameIdentifier.of(
        metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME, user);
  }

  public static NameIdentifier ofMetalakeCreateRole() {
    return NameIdentifier.of(
        Entity.SYSTEM_METALAKE_RESERVED_NAME,
        Entity.SYSTEM_CATALOG_RESERVED_NAME,
        Entity.ROLE_SCHEMA_NAME,
        Entity.METALAKE_CREATE_ROLE);
  }

  public static NameIdentifier ofSystemMetalakeAddUserRole() {
    return NameIdentifier.of(
        Entity.SYSTEM_METALAKE_RESERVED_NAME,
        Entity.SYSTEM_CATALOG_RESERVED_NAME,
        Entity.ROLE_SCHEMA_NAME,
        Entity.SYSTEM_METALAKE_MANAGE_USER_ROLE);
  }

  public static Namespace ofRoleNamespace(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME);
  }

  public static Namespace ofGroupNamespace(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME);
  }

  public static Namespace ofUserNamespace(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME);
  }

  public static void checkUser(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "User identifier must not be null");
    checkUserNamespace(ident.namespace());
  }

  public static void checkGroup(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Group identifier must not be null");
    checkGroupNamespace(ident.namespace());
  }

  public static void checkRole(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Role identifier must not be null");
    checkRoleNamespace(ident.namespace());
  }

  public static void checkUserNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 3,
        "User namespace must have 3 levels, the input namespace is %s",
        namespace);
  }

  public static void checkGroupNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 3,
        "Group namespace must have 3 levels, the input namespace is %s",
        namespace);
  }

  public static void checkRoleNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 3,
        "Role namespace must have 3 levels, the input namespace is %s",
        namespace);
  }

  public static void checkPermission(String metalake, SecurableObject object) {

    if (!satisfyPrivileges(metalake, object)) {
      throw new ForbiddenException(
          "Securable object %s doesn't have the one of privileges %s",
          object,
          StringUtils.join(
              object.privileges().stream().map(Privilege::name).collect(Collectors.toList()), ","));
    }
  }

  public static boolean satisfyPrivileges(String metalake, SecurableObject object) {
    if (object.privileges().isEmpty()) {
      return true;
    }

    String currentUser = PrincipalUtils.getCurrentUserName();

    AccessControlManager accessControlManager = GravitinoEnv.getInstance().accessControlManager();
    List<RoleEntity> roles;
    try {
      roles = accessControlManager.getRolesByUserFromMetalake(metalake, currentUser);

      for (RoleEntity role : roles) {
        for (Privilege privilege : object.privileges()) {
          if (role.hasPrivilegeWithCondition(object, privilege.name(), Privilege.Condition.DENY)) {
            continue;
          }

          if (role.hasPrivilegeWithCondition(object, privilege.name(), Privilege.Condition.ALLOW)) {
            return true;
          }
        }
      }
    } catch (NoSuchUserException noSuchUserException) {
      return false;
    }

    return false;
  }

  public static Void checkMetalakeExists(String metalake) throws NoSuchMetalakeException {
    try {
      EntityStore store = GravitinoEnv.getInstance().entityStore();

      NameIdentifier metalakeIdent = NameIdentifier.of(metalake);
      if (!store.exists(metalakeIdent, Entity.EntityType.METALAKE)) {
        LOG.warn("Metalake {} does not exist", metalakeIdent);
        throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalakeIdent);
      }
      return null;
    } catch (IOException e) {
      LOG.error("Failed to do storage operation", e);
      throw new RuntimeException(e);
    }
  }
}
