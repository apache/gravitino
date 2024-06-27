/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.MetadataObjects.METADATA_OBJECT_RESERVED_NAME;

import com.datastrato.gravitino.exceptions.ForbiddenException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.base.Splitter;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** This class is used for checking permission of every operation. * */
public class PermissionChecker {
  private PermissionChecker() {}

  private static final Splitter DOT_SPLITTER = Splitter.on('.');

  public static void check(String metalake, SecurableObject object) {

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
