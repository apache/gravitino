/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.MetadataObjects.METADATA_OBJECT_RESERVED_NAME;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.ForbiddenException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
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

  private static final Splitter DOT_SPLITTER = Splitter.on('.');

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
        Entity.MANAGE_METALAKE_ADMIN_ROLE);
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

    if (!satisfyOnePrivilegeOfSecurableObject(metalake, object)) {
      throw new ForbiddenException(
          "Securable object %s doesn't have the one of privileges %s",
          object,
          StringUtils.join(
              object.privileges().stream().map(Privilege::name).collect(Collectors.toList()), ","));
    }
  }

  public static boolean satisfyOnePrivilegeOfSecurableObject(
      String metalake, SecurableObject object) {
    if (object.privileges().isEmpty()) {
      return true;
    }

    String currentUser = PrincipalUtils.getCurrentUserName();

    AccessControlManager accessControlManager = GravitinoEnv.getInstance().accessControlManager();
    List<RoleEntity> roles;
    try {
      roles = accessControlManager.listRolesByUser(metalake, currentUser);
    } catch (NoSuchUserException noSuchUserException) {
      // If one user doesn't exist in the metalake, the user doesn't have any privilege.
      return false;
    }

    // If one privilege of securable object is satisfied, the operation will be allowed.
    // For example, The `loadTable` operation will be allowed when the user has the privilege
    // of reading table or writing table. If the user is allowed to read table, but he is
    // denied to write table, he can still load table.
    for (Privilege privilege : object.privileges()) {
      boolean privilegeDenied = false;
      for (RoleEntity role : roles) {
        // The deny privilege is prior to the allow privilege. If one entity has the
        // deny privilege and allow privilege at the same time. The entity doesn't have the
        // privilege.
        if (hasPrivilegeWithCondition(role, object, privilege.name(), Privilege.Condition.DENY)) {
          privilegeDenied = true;
          break;
        }
      }
      if (privilegeDenied) {
        continue;
      }

      for (RoleEntity role : roles) {
        if (hasPrivilegeWithCondition(role, object, privilege.name(), Privilege.Condition.ALLOW)) {
          return true;
        }
      }
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

  private static boolean hasPrivilegeWithCondition(
      RoleEntity roleEntity,
      MetadataObject targetObject,
      Privilege.Name targetPrivilege,
      Privilege.Condition condition) {
    String metalake = roleEntity.namespace().level(0);
    for (SecurableObject object : roleEntity.securableObjects()) {
      // If one entity's parent has the privilege, the entity will have the privilege.
      if (isSameOrParent(
          convertInnerObject(metalake, object), convertInnerObject(metalake, targetObject))) {
        for (Privilege privilege : object.privileges()) {
          if (privilege.name().equals(targetPrivilege) && privilege.condition().equals(condition)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static InnerSecurableObject convertInnerObject(
      String metalake, MetadataObject securableObject) {
    if (MetadataObject.Type.METALAKE.equals(securableObject.type())) {
      if (METADATA_OBJECT_RESERVED_NAME.equals(securableObject.name())) {
        return ROOT;
      } else {
        return new InnerSecurableObject(ROOT, securableObject.name());
      }
    }

    InnerSecurableObject currentObject = new InnerSecurableObject(ROOT, metalake);
    List<String> names = DOT_SPLITTER.splitToList(securableObject.fullName());
    for (String name : names) {
      currentObject = new InnerSecurableObject(currentObject, name);
    }
    return currentObject;
  }

  private static final InnerSecurableObject ROOT = new InnerSecurableObject(null, "*");

  private static class InnerSecurableObject {
    InnerSecurableObject parent;
    String name;

    InnerSecurableObject(InnerSecurableObject parent, String name) {
      this.parent = parent;
      this.name = name;
    }

    public InnerSecurableObject getParent() {
      return parent;
    }

    public String getName() {
      return name;
    }

    @Override
    public int hashCode() {
      return Objects.hash(parent, name);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof InnerSecurableObject)) {
        return false;
      }

      InnerSecurableObject innerSecurableObject = (InnerSecurableObject) obj;
      return Objects.equals(this.name, innerSecurableObject.name)
          && Objects.equals(this.parent, innerSecurableObject.parent);
    }
  }

  private static boolean isSameOrParent(InnerSecurableObject object, InnerSecurableObject target) {
    if (object.equals(target)) {
      return true;
    }

    if (target.getParent() != null) {
      return isSameOrParent(object, target.getParent());
    }

    return false;
  }
}
