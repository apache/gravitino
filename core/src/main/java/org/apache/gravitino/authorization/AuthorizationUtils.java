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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalPrivilegeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* The utilization class of authorization module*/
public class AuthorizationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationUtils.class);
  static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in the metalake %s";
  static final String GROUP_DOES_NOT_EXIST_MSG = "Group %s does not exist in the metalake %s";
  static final String ROLE_DOES_NOT_EXIST_MSG = "Role %s does not exist in the metalake %s";

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
      BiConsumer<AuthorizationPlugin, String> consumer) {
    Set<String> catalogsAlreadySet = Sets.newHashSet();
    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();
    for (SecurableObject securableObject : securableObjects) {
      if (needApplyAuthorizationPluginAllCatalogs(securableObject)) {
        NameIdentifier[] catalogs = catalogManager.listCatalogs(Namespace.of(metalake));
        // ListCatalogsInfo return `CatalogInfo` instead of `BaseCatalog`, we need `BaseCatalog` to
        // call authorization plugin method.
        for (NameIdentifier catalog : catalogs) {
          callAuthorizationPluginImpl(consumer, catalogManager.loadCatalog(catalog));
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
    List<Catalog> loadedCatalogs = loadMetadataObjectCatalog(metalake, metadataObject);
    for (Catalog catalog : loadedCatalogs) {
      callAuthorizationPluginImpl(consumer, catalog);
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

  public static void authorizationPluginRemovePrivileges(
      NameIdentifier ident, Entity.EntityType type, List<String> locations) {
    // If we enable authorization, we should remove the privileges about the entity in the
    // authorization plugin.
    if (GravitinoEnv.getInstance().accessControlDispatcher() != null) {
      MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(ident, type);
      String metalake =
          type == Entity.EntityType.METALAKE ? ident.name() : ident.namespace().level(0);

      MetadataObjectChange removeObject = MetadataObjectChange.remove(metadataObject, locations);
      callAuthorizationPluginForMetadataObject(
          metalake,
          metadataObject,
          authorizationPlugin -> {
            authorizationPlugin.onMetadataUpdated(removeObject);
          });
    }
  }

  public static void removeCatalogPrivileges(Catalog catalog, List<String> locations) {
    // If we enable authorization, we should remove the privileges about the entity in the
    // authorization plugin.
    MetadataObject metadataObject =
        MetadataObjects.of(null, catalog.name(), MetadataObject.Type.CATALOG);
    MetadataObjectChange removeObject = MetadataObjectChange.remove(metadataObject, locations);

    callAuthorizationPluginImpl(
        authorizationPlugin -> {
          authorizationPlugin.onMetadataUpdated(removeObject);
        },
        catalog);
  }

  public static void authorizationPluginRenamePrivileges(
      NameIdentifier ident, Entity.EntityType type, String newName) {
    // If we enable authorization, we should rename the privileges about the entity in the
    // authorization plugin.
    if (GravitinoEnv.getInstance().accessControlDispatcher() != null) {
      MetadataObject oldMetadataObject = NameIdentifierUtil.toMetadataObject(ident, type);
      MetadataObject newMetadataObject =
          NameIdentifierUtil.toMetadataObject(NameIdentifier.of(ident.namespace(), newName), type);
      MetadataObjectChange renameObject =
          MetadataObjectChange.rename(oldMetadataObject, newMetadataObject);

      String metalake = type == Entity.EntityType.METALAKE ? newName : ident.namespace().level(0);

      // For a renamed catalog, we should pass the new name catalog, otherwise we can't find the
      // catalog in the entity store
      callAuthorizationPluginForMetadataObject(
          metalake,
          newMetadataObject,
          authorizationPlugin -> {
            authorizationPlugin.onMetadataUpdated(renameObject);
          });
    }
  }

  public static Role filterSecurableObjects(
      RoleEntity role, String metalakeName, String catalogName) {
    List<SecurableObject> securableObjects = role.securableObjects();
    List<SecurableObject> filteredSecurableObjects = Lists.newArrayList();
    for (SecurableObject securableObject : securableObjects) {
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalakeName, securableObject);
      if (securableObject.type() == MetadataObject.Type.METALAKE) {
        filteredSecurableObjects.add(securableObject);
      } else {
        NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);

        if (catalogIdent.name().equals(catalogName)) {
          filteredSecurableObjects.add(securableObject);
        }
      }
    }

    return RoleEntity.builder()
        .withId(role.id())
        .withName(role.name())
        .withAuditInfo(role.auditInfo())
        .withNamespace(role.namespace())
        .withSecurableObjects(filteredSecurableObjects)
        .withProperties(role.properties())
        .build();
  }

  private static boolean needApplyAuthorizationPluginAllCatalogs(MetadataObject.Type type) {
    return type == MetadataObject.Type.METALAKE;
  }

  private static boolean needApplyAuthorization(MetadataObject.Type type) {
    return type != MetadataObject.Type.ROLE && type != MetadataObject.Type.METALAKE;
  }

  private static void callAuthorizationPluginImpl(
      BiConsumer<AuthorizationPlugin, String> consumer, Catalog catalog) {

    if (catalog instanceof BaseCatalog) {
      BaseCatalog baseCatalog = (BaseCatalog) catalog;
      if (baseCatalog.getAuthorizationPlugin() != null) {
        consumer.accept(baseCatalog.getAuthorizationPlugin(), catalog.name());
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Catalog %s is not a BaseCatalog, we don't support authorization plugin for it",
              catalog.type()));
    }
  }

  private static void callAuthorizationPluginImpl(
      Consumer<AuthorizationPlugin> consumer, Catalog catalog) {

    if (catalog instanceof BaseCatalog) {
      BaseCatalog baseCatalog = (BaseCatalog) catalog;
      if (baseCatalog.getAuthorizationPlugin() != null) {
        consumer.accept(baseCatalog.getAuthorizationPlugin());
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Catalog %s is not a BaseCatalog, we don't support authorization plugin for it",
              catalog.type()));
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

  private static List<Catalog> loadMetadataObjectCatalog(
      String metalake, MetadataObject metadataObject) {
    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();
    List<Catalog> loadedCatalogs = Lists.newArrayList();
    if (needApplyAuthorizationPluginAllCatalogs(metadataObject.type())) {
      NameIdentifier[] catalogs = catalogManager.listCatalogs(Namespace.of(metalake));
      // ListCatalogsInfo return `CatalogInfo` instead of `BaseCatalog`, we need `BaseCatalog` to
      // call authorization plugin method.
      for (NameIdentifier catalog : catalogs) {
        loadedCatalogs.add(catalogManager.loadCatalog(catalog));
      }
    } else if (needApplyAuthorization(metadataObject.type())) {
      NameIdentifier catalogIdent =
          NameIdentifierUtil.getCatalogIdentifier(
              MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
      Catalog catalog = catalogManager.loadCatalog(catalogIdent);
      loadedCatalogs.add(catalog);
    }

    return loadedCatalogs;
  }

  // The Hive default schema location is Hive warehouse directory
  private static String getHiveDefaultLocation(String metalakeName, String catalogName) {
    NameIdentifier defaultSchemaIdent =
        NameIdentifier.of(metalakeName, catalogName, "default" /*Hive default schema*/);
    Schema schema = GravitinoEnv.getInstance().schemaDispatcher().loadSchema(defaultSchemaIdent);
    if (schema.properties().containsKey(HiveConstants.LOCATION)) {
      String defaultSchemaLocation = schema.properties().get(HiveConstants.LOCATION);
      if (defaultSchemaLocation != null && !defaultSchemaLocation.isEmpty()) {
        return defaultSchemaLocation;
      } else {
        LOG.warn("Schema {} location is not found", defaultSchemaIdent);
      }
    }

    return null;
  }

  public static List<String> getMetadataObjectLocation(
      NameIdentifier ident, Entity.EntityType type) {
    List<String> locations = new ArrayList<>();
    try {
      switch (type) {
        case METALAKE:
          {
            NameIdentifier[] identifiers =
                GravitinoEnv.getInstance()
                    .catalogDispatcher()
                    .listCatalogs(Namespace.of(ident.name()));
            Arrays.stream(identifiers)
                .collect(Collectors.toList())
                .forEach(
                    identifier -> {
                      Catalog catalogObj =
                          GravitinoEnv.getInstance().catalogDispatcher().loadCatalog(identifier);
                      if (catalogObj.provider().equals("hive")) {
                        // The Hive default schema location is Hive warehouse directory
                        String defaultSchemaLocation =
                            getHiveDefaultLocation(ident.name(), catalogObj.name());
                        if (defaultSchemaLocation != null && !defaultSchemaLocation.isEmpty()) {
                          locations.add(defaultSchemaLocation);
                        }
                      }
                    });
          }
          break;
        case CATALOG:
          {
            Catalog catalogObj = GravitinoEnv.getInstance().catalogDispatcher().loadCatalog(ident);
            if (catalogObj.provider().equals("hive")) {
              // The Hive default schema location is Hive warehouse directory
              String defaultSchemaLocation =
                  getHiveDefaultLocation(ident.namespace().level(0), ident.name());
              if (defaultSchemaLocation != null && !defaultSchemaLocation.isEmpty()) {
                locations.add(defaultSchemaLocation);
              }
            }
          }
          break;
        case SCHEMA:
          {
            Catalog catalogObj =
                GravitinoEnv.getInstance()
                    .catalogDispatcher()
                    .loadCatalog(
                        NameIdentifier.of(ident.namespace().level(0), ident.namespace().level(1)));
            LOG.info("Catalog provider is %s", catalogObj.provider());
            if (catalogObj.provider().equals("hive")) {
              Schema schema = GravitinoEnv.getInstance().schemaDispatcher().loadSchema(ident);
              if (schema.properties().containsKey(HiveConstants.LOCATION)) {
                String schemaLocation = schema.properties().get(HiveConstants.LOCATION);
                if (StringUtils.isNotBlank(schemaLocation)) {
                  locations.add(schemaLocation);
                } else {
                  LOG.warn("Schema {} location is not found", ident);
                }
              }
            }
            // TODO: [#6133] Supports get Fileset schema location in the AuthorizationUtils
          }
          break;
        case TABLE:
          {
            Catalog catalogObj =
                GravitinoEnv.getInstance()
                    .catalogDispatcher()
                    .loadCatalog(
                        NameIdentifier.of(ident.namespace().level(0), ident.namespace().level(1)));
            if (catalogObj.provider().equals("hive")) {
              Table table = GravitinoEnv.getInstance().tableDispatcher().loadTable(ident);
              if (table.properties().containsKey(HiveConstants.LOCATION)) {
                String tableLocation = table.properties().get(HiveConstants.LOCATION);
                if (StringUtils.isNotBlank(tableLocation)) {
                  locations.add(tableLocation);
                } else {
                  LOG.warn("Table {} location is not found", ident);
                }
              }
            }
          }
          break;
        case FILESET:
          FilesetDispatcher filesetDispatcher = GravitinoEnv.getInstance().filesetDispatcher();
          Fileset fileset = filesetDispatcher.loadFileset(ident);
          Preconditions.checkArgument(
              fileset != null, String.format("Fileset %s is not found", ident));
          String filesetLocation = fileset.storageLocation();
          Preconditions.checkArgument(
              filesetLocation != null, String.format("Fileset %s location is not found", ident));
          locations.add(filesetLocation);
          break;
        default:
          throw new AuthorizationPluginException(
              "Failed to get location paths for metadata object %s type %s", ident, type);
      }
    } catch (Exception e) {
      LOG.warn("Failed to get location paths for metadata object {} type {}", ident, type, e);
    }

    return locations;
  }

  private static NameIdentifier getObjectNameIdentifier(
      String metalake, MetadataObject metadataObject) {
    return NameIdentifier.parse(String.format("%s.%s", metalake, metadataObject.fullName()));
  }
}
