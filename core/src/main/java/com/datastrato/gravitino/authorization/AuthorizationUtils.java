/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* The utilization class of authorization module*/
public class AuthorizationUtils {

  static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in th metalake %s";
  static final String GROUP_DOES_NOT_EXIST_MSG = "Group %s does not exist in th metalake %s";
  static final String ROLE_DOES_NOT_EXIST_MSG = "Role %s does not exist in th metalake %s";

  private static final String UNSUPPORTED_ERROR_MSG = "%s is an unsupported catalog type";
  private static final List<Privilege> CATALOG_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.LoadCatalog.get(),
          Privileges.LoadCatalog.get(),
          Privileges.AlterCatalog.get(),
          Privileges.CreateCatalog.get(),
          Privileges.DropCatalog.get());
  private static final List<Privilege> CATALOGS_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(CATALOG_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListCatalog.get())
          .build();
  private static final List<Privilege> SCHEMA_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.LoadSchema.get(),
          Privileges.AlterSchema.get(),
          Privileges.CreateSchema.get(),
          Privileges.DropSchema.get());
  private static final List<Privilege> SCHEMA_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(SCHEMA_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListSchema.get())
          .build();
  private static final List<Privilege> TABLE_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.AlterTable.get(),
          Privileges.CreateTable.get(),
          Privileges.DropTable.get(),
          Privileges.ReadTable.get(),
          Privileges.WriteTable.get());
  private static final List<Privilege> TABLE_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(TABLE_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListTable.get())
          .build();
  private static final List<Privilege> FILESET_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.AlterFileset.get(),
          Privileges.CreateFileset.get(),
          Privileges.DropFileset.get(),
          Privileges.ReadFileset.get(),
          Privileges.WriteFileset.get());
  private static final List<Privilege> FILESET_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(FILESET_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListFileset.get())
          .build();
  private static final List<Privilege> TOPIC_PRIVILEGES_EXCEPT_FOR_LIST =
      ImmutableList.of(
          Privileges.AlterTopic.get(),
          Privileges.CreateTopic.get(),
          Privileges.DropTopic.get(),
          Privileges.ReadTopic.get(),
          Privileges.WriteTopic.get());
  private static final List<Privilege> TOPIC_PRIVILEGES =
      ImmutableList.<Privilege>builder()
          .addAll(TOPIC_PRIVILEGES_EXCEPT_FOR_LIST)
          .add(Privileges.ListTopic.get())
          .build();

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

  public static Namespace ofRoleNamespace(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME);
  }

  public static Namespace ofGroupNamespace(String metalake) {
    return Namespace.of(
        metalake, CatalogEntity.SYSTEM_CATALOG_RESERVED_NAME, SchemaEntity.GROUP_SCHEMA_NAME);
  }

  public static Namespace ofUserNamespace(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME);
  }

  /**
   * * There are 6 kinds of entities for the privileges: <br>
   * `*` has all the catalog operation privileges. <br>
   * `catalog` has the catalog operation privileges except for list operation and all the schema
   * operation privileges. <br>
   * `catalog.schema` has the schema operation privileges except for list operation and all the
   * table/topic/fileset privileges. <br>
   * `catalog.schema.table` has the table operation privileges except for list operation. <br>
   * `catalog.schema.topic` has the topic operation privileges except for list operation. <br>
   * `catalog.schema.fileset` has the fileset operation privileges except for list operation. <br>
   * The entity can only add the children entity privilege of list operation.
   */
  public static void checkSecurableObjectPrivileges(
      SecurableObject securableObject, Catalog.Type type, List<Privilege> privileges) {
    List<Privilege> supportedPrivileges = getEntitySupportedPrivileges(securableObject, type);
    for (Privilege privilege : privileges) {
      if (!supportedPrivileges.contains(privilege)) {
        throw new IllegalArgumentException(
            String.format("%s shouldn't bind the privilege %s", securableObject, privilege));
      }
    }
  }

  public static boolean isAllCatalogs(SecurableObject securableObject) {
    if (securableObject.parent() == null && "*".equals(securableObject.name())) {
      return true;
    }

    return false;
  }

  public static boolean isSingleCatalog(SecurableObject securableObject) {
    if (securableObject.parent() == null && !"*".equals(securableObject.name())) {
      return true;
    }

    return false;
  }

  public static String getCatalogName(SecurableObject securableObject) {
    if (securableObject.parent() == null) {
      return securableObject.name();
    }

    return getCatalogName(securableObject.parent());
  }

  private static List<Privilege> getLeavesEntityPrivilegesExceptForList(Catalog.Type type) {
    switch (type) {
      case FILESET:
        return FILESET_PRIVILEGES_EXCEPT_FOR_LIST;

      case RELATIONAL:
        return TABLE_PRIVILEGES_EXCEPT_FOR_LIST;

      case MESSAGING:
        return TOPIC_PRIVILEGES_EXCEPT_FOR_LIST;

      default:
        throw new IllegalArgumentException(String.format(UNSUPPORTED_ERROR_MSG, type));
    }
  }

  private static List<Privilege> getEntitySupportedPrivileges(
      SecurableObject securableObject, Catalog.Type type) {
    if (isAllCatalogs(securableObject)) {
      return ImmutableList.<Privilege>builder().addAll(CATALOGS_PRIVILEGES).build();
    }

    if (isSingleCatalog(securableObject)) {
      return ImmutableList.<Privilege>builder()
          .addAll(CATALOG_PRIVILEGES_EXCEPT_FOR_LIST)
          .addAll(SCHEMA_PRIVILEGES)
          .build();
    }

    if (isSchema(securableObject)) {
      return ImmutableList.<Privilege>builder()
          .addAll(SCHEMA_PRIVILEGES_EXCEPT_FOR_LIST)
          .addAll(getLeavesEntityPrivileges(type))
          .build();
    }

    return getLeavesEntityPrivilegesExceptForList(type);
  }

  private static List<Privilege> getLeavesEntityPrivileges(Catalog.Type type) {
    switch (type) {
      case FILESET:
        return FILESET_PRIVILEGES;

      case RELATIONAL:
        return TABLE_PRIVILEGES;

      case MESSAGING:
        return TOPIC_PRIVILEGES;

      default:
        throw new IllegalArgumentException(String.format(UNSUPPORTED_ERROR_MSG, type));
    }
  }

  private static boolean isSchema(SecurableObject securableObject) {
    return securableObject.parent().parent() == null;
  }
}
