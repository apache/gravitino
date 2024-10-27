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
package org.apache.gravitino.authorization;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalPrivilegeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

/* The utilization class of authorization module*/
public class AuthorizationUtils {

  static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in th metalake %s";
  static final String GROUP_DOES_NOT_EXIST_MSG = "Group %s does not exist in th metalake %s";
  static final String ROLE_DOES_NOT_EXIST_MSG = "Role %s does not exist in th metalake %s";

  private static final Set<Privilege.Name> FILESET_PRIVILEGES =
      Sets.immutableEnumSet(
          Privilege.Name.CREATE_FILESET, Privilege.Name.WRITE_FILESET, Privilege.Name.READ_FILESET);
  private static final Set<Privilege.Name> TABLE_PRIVILEGES =
      Sets.immutableEnumSet(
          Privilege.Name.CREATE_TABLE, Privilege.Name.MODIFY_TABLE, Privilege.Name.SELECT_TABLE);
  private static final Set<Privilege.Name> TOPIC_PRIVILEGES =
      Sets.immutableEnumSet(
          Privilege.Name.CREATE_TOPIC, Privilege.Name.PRODUCE_TOPIC, Privilege.Name.CONSUME_TOPIC);

  private AuthorizationUtils() {}

  public static void checkCurrentUser(String metalake, String user) {
    try {
      AccessControlDispatcher dispatcher = GravitinoEnv.getInstance().accessControlDispatcher();
      // Only when we enable authorization, we need to check the current user
      if (dispatcher != null) {
        dispatcher.getUser(metalake, user);
      }
    } catch (NoSuchUserException nsu) {
      throw new ForbiddenException(
          "Current user %s doesn't exist in the metalake %s, you should add the user to the metalake first",
          user, metalake);
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

  // Every catalog has one authorization plugin, we should avoid calling
  // underlying authorization repeatedly. So we use a set to record which
  // catalog has been called the authorization plugin.
  public static void callAuthorizationPluginForSecurableObjects(
      String metalake,
      List<SecurableObject> securableObjects,
      Set<String> catalogsAlreadySet,
      Consumer<AuthorizationPlugin> consumer) {
    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();
    for (SecurableObject securableObject : securableObjects) {
      if (needApplyAuthorizationPluginAllCatalogs(securableObject)) {
        Catalog[] catalogs = catalogManager.listCatalogsInfo(Namespace.of(metalake));
        for (Catalog catalog : catalogs) {
          callAuthorizationPluginImpl(consumer, catalog);
        }

      } else if (needApplyAuthorization(securableObject.type())) {
        NameIdentifier catalogIdent =
            NameIdentifierUtil.getCatalogIdentifier(
                MetadataObjectUtil.toEntityIdent(metalake, securableObject));
        Catalog catalog = catalogManager.loadCatalog(catalogIdent);
        if (!catalogsAlreadySet.contains(catalog.name())) {
          catalogsAlreadySet.add(catalog.name());
          callAuthorizationPluginImpl(consumer, catalog);
        }
      }
    }
  }

  public static void callAuthorizationPluginForMetadataObject(
      String metalake, MetadataObject metadataObject, Consumer<AuthorizationPlugin> consumer) {
    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();
    if (needApplyAuthorizationPluginAllCatalogs(metadataObject.type())) {
      Catalog[] catalogs = catalogManager.listCatalogsInfo(Namespace.of(metalake));
      for (Catalog catalog : catalogs) {
        callAuthorizationPluginImpl(consumer, catalog);
      }
    } else if (needApplyAuthorization(metadataObject.type())) {
      NameIdentifier catalogIdent =
          NameIdentifierUtil.getCatalogIdentifier(
              MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
      Catalog catalog = catalogManager.loadCatalog(catalogIdent);
      callAuthorizationPluginImpl(consumer, catalog);
    }
  }

  private static void callAuthorizationPluginImpl(
      Consumer<AuthorizationPlugin> consumer, Catalog catalog) {

    if (catalog instanceof BaseCatalog) {
      BaseCatalog baseCatalog = (BaseCatalog) catalog;
      if (baseCatalog.getAuthorizationPlugin() != null) {
        consumer.accept(baseCatalog.getAuthorizationPlugin());
      }
    }
  }

  public static boolean needApplyAuthorizationPluginAllCatalogs(SecurableObject securableObject) {
    if (securableObject.type() == MetadataObject.Type.METALAKE) {
      List<Privilege> privileges = securableObject.privileges();
      for (Privilege privilege : privileges) {
        if (privilege.canBindTo(MetadataObject.Type.CATALOG)) {
          return true;
        }
      }
    }
    return false;
  }

  public static void checkDuplicatedNamePrivilege(Collection<Privilege> privileges) {
    Set<Privilege.Name> privilegeNameSet = Sets.newHashSet();
    for (Privilege privilege : privileges) {
      if (privilegeNameSet.contains(privilege.name())) {
        throw new IllegalPrivilegeException(
            "Doesn't support duplicated privilege name %s with different condition",
            privilege.name());
      }
      privilegeNameSet.add(privilege.name());
    }
  }

  public static void checkPrivilege(
      PrivilegeDTO privilegeDTO, MetadataObject object, String metalake) {
    Privilege privilege = DTOConverters.fromPrivilegeDTO(privilegeDTO);

    if (!privilege.canBindTo(object.type())) {
      throw new IllegalPrivilegeException(
          "Securable object %s type %s don't support binding privilege %s",
          object.fullName(), object.type(), privilege);
    }

    if (object.type() == MetadataObject.Type.CATALOG
        || object.type() == MetadataObject.Type.SCHEMA) {
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, object);
      NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
      try {
        if (FILESET_PRIVILEGES.contains(privilege.name())) {
          checkCatalogType(catalogIdent, Catalog.Type.FILESET, privilege);
        }

        if (TABLE_PRIVILEGES.contains(privilege.name())) {
          checkCatalogType(catalogIdent, Catalog.Type.RELATIONAL, privilege);
        }

        if (TOPIC_PRIVILEGES.contains(privilege.name())) {
          checkCatalogType(catalogIdent, Catalog.Type.MESSAGING, privilege);
        }
      } catch (NoSuchCatalogException ne) {
        throw new NoSuchMetadataObjectException(
            "Securable object %s doesn't exist", object.fullName());
      }
    }
  }

  private static void checkCatalogType(
      NameIdentifier catalogIdent, Catalog.Type type, Privilege privilege) {
    Catalog catalog = GravitinoEnv.getInstance().catalogDispatcher().loadCatalog(catalogIdent);
    if (catalog.type() != type) {
      throw new IllegalPrivilegeException(
          "Catalog %s type %s doesn't support privilege %s",
          catalogIdent, catalog.type(), privilege);
    }
  }

  private static boolean needApplyAuthorizationPluginAllCatalogs(MetadataObject.Type type) {
    return type == MetadataObject.Type.METALAKE;
  }

  private static boolean needApplyAuthorization(MetadataObject.Type type) {
    return type != MetadataObject.Type.ROLE && type != MetadataObject.Type.METALAKE;
  }
}
