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
package org.apache.gravitino.authorization.ranger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.common.ErrorMessages;
import org.apache.gravitino.authorization.common.PathBasedMetadataObject;
import org.apache.gravitino.authorization.common.PathBasedSecurableObject;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerAuthorizationHDFSPlugin extends RangerAuthorizationPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationHDFSPlugin.class);
  private static final Pattern HDFS_PATTERN = Pattern.compile("^hdfs://[^/]*");
  // Securable object types that have storage locations.
  private static final Set<MetadataObject.Type> LOCATED_TYPES =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG,
          MetadataObject.Type.SCHEMA,
          MetadataObject.Type.TABLE,
          MetadataObject.Type.FILESET);

  public RangerAuthorizationHDFSPlugin(String metalake, Map<String, String> config) {
    super(metalake, config);
  }

  @Override
  public Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.USE_CATALOG,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.CREATE_CATALOG,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.USE_SCHEMA,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.CREATE_SCHEMA,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.CREATE_TABLE,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.MODIFY_TABLE,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.SELECT_TABLE,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.READ_FILESET,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.WRITE_FILESET,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.CREATE_FILESET,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE));
  }

  @Override
  public Set<AuthorizationPrivilege> ownerMappingRule() {
    return ImmutableSet.of(
        RangerPrivileges.RangerHdfsPrivilege.READ,
        RangerPrivileges.RangerHdfsPrivilege.WRITE,
        RangerPrivileges.RangerHdfsPrivilege.EXECUTE);
  }

  @Override
  public List<String> policyResourceDefinesRule() {
    return ImmutableList.of(RangerDefines.PolicyResource.PATH.getName());
  }

  @Override
  public boolean requiresLocations() {
    return true;
  }

  String getAuthorizationPath(PathBasedMetadataObject pathBasedMetadataObject) {
    return HDFS_PATTERN.matcher(pathBasedMetadataObject.path()).replaceAll("");
  }

  /**
   * Find the managed policy for the ranger securable object.
   *
   * @param authzMetadataObject The ranger securable object to find the managed policy.
   * @return The managed policy for the metadata object.
   */
  @Override
  public RangerPolicy findManagedPolicy(AuthorizationMetadataObject authzMetadataObject)
      throws AuthorizationPluginException {
    List<String> nsMetadataObj = authzMetadataObject.names();
    PathBasedMetadataObject pathAuthzMetadataObject = (PathBasedMetadataObject) authzMetadataObject;
    Map<String, String> preciseFilters = new HashMap<>();
    for (int i = 0; i < nsMetadataObj.size() && i < policyResourceDefinesRule().size(); i++) {
      preciseFilters.put(
          policyResourceDefinesRule().get(i), getAuthorizationPath(pathAuthzMetadataObject));
    }
    return preciseFindPolicy(authzMetadataObject, preciseFilters);
  }

  @Override
  /** Wildcard search the Ranger policies in the different Ranger service. */
  protected List<RangerPolicy> wildcardSearchPolicies(
      AuthorizationMetadataObject authzMetadataObject) {
    Preconditions.checkArgument(authzMetadataObject instanceof PathBasedMetadataObject);
    PathBasedMetadataObject pathBasedMetadataObject = (PathBasedMetadataObject) authzMetadataObject;
    List<String> resourceDefines = policyResourceDefinesRule();
    Map<String, String> searchFilters = new HashMap<>();
    searchFilters.put(SearchFilter.SERVICE_NAME, rangerServiceName);
    resourceDefines.forEach(
        resourceDefine -> {
          searchFilters.put(
              SearchFilter.RESOURCE_PREFIX + resourceDefine,
              getAuthorizationPath(pathBasedMetadataObject));
        });
    try {
      return rangerClient.findPolicies(searchFilters);
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e, "Failed to find policies in Ranger");
    }
  }

  /**
   * If rename the SCHEMA, Need to rename these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` <br>
   * If rename the TABLE, Need to rename these the relevant policies, `{schema}.*`, `{schema}.*.*`
   * <br>
   */
  @Override
  protected void renameMetadataObject(
      AuthorizationMetadataObject authzMetadataObject,
      AuthorizationMetadataObject newAuthzMetadataObject) {
    Preconditions.checkArgument(
        authzMetadataObject instanceof PathBasedMetadataObject,
        "The metadata object must be a PathBasedMetadataObject");
    Preconditions.checkArgument(
        newAuthzMetadataObject instanceof PathBasedMetadataObject,
        "The metadata object must be a PathBasedMetadataObject");
    updatePolicyByMetadataObject(
        newAuthzMetadataObject.type().metadataObjectType(),
        authzMetadataObject,
        newAuthzMetadataObject);
  }

  @Override
  protected void updatePolicyByMetadataObject(
      MetadataObject.Type operationType,
      AuthorizationMetadataObject oldAuthzMetaObject,
      AuthorizationMetadataObject newAuthzMetaObject) {
    List<RangerPolicy> oldPolicies = wildcardSearchPolicies(oldAuthzMetaObject);
    List<RangerPolicy> existNewPolicies = wildcardSearchPolicies(newAuthzMetaObject);
    if (oldPolicies.isEmpty()) {
      LOG.warn("Cannot find the Ranger policy for the metadata object({})!", oldAuthzMetaObject);
      return;
    }
    if (!existNewPolicies.isEmpty()) {
      LOG.warn("The Ranger policy for the metadata object({}) already exists!", newAuthzMetaObject);
    }
    oldPolicies.forEach(
        policy -> {
          try {
            // Update the policy name is following Gravitino's spec
            // Only Hive managed table rename will use this case
            String oldResource =
                policy
                    .getResources()
                    .get(rangerHelper.policyResourceDefines.get(0))
                    .getValues()
                    .get(0);
            List<String> oldResourceNames =
                Arrays.stream(oldResource.split("/"))
                    .filter(path -> StringUtils.isNotBlank(path) && !".".equals(path))
                    .collect(Collectors.toList());
            List<String> newResourceNames =
                Arrays.stream(
                        getAuthorizationPath((PathBasedMetadataObject) newAuthzMetaObject)
                            .split("/"))
                    .filter(path -> StringUtils.isNotBlank(path) && !".".equals(path))
                    .collect(Collectors.toList());

            int minLen = Math.min(oldResourceNames.size(), newResourceNames.size());
            for (int i = 0; i < minLen; i++) {
              String oldName = oldResourceNames.get(i);
              String newName = newResourceNames.get(i);
              if (!oldName.equals(newName)) {
                if (oldName.equals(oldAuthzMetaObject.name())
                    && newName.equals(newAuthzMetaObject.name())) {
                  oldResourceNames.set(i, newAuthzMetaObject.name());
                  break;
                } else {
                  // If resource doesn't match, ignore this resource
                  return;
                }
              }
            }
            String newResourcePath = "/" + String.join("/", oldResourceNames);

            policy.setName(newResourcePath);
            // Update the policy resource name to new name
            policy
                .getResources()
                .put(
                    rangerHelper.policyResourceDefines.get(0),
                    new RangerPolicy.RangerPolicyResource(newResourcePath));

            boolean alreadyExist =
                existNewPolicies.stream()
                    .anyMatch(
                        existNewPolicy ->
                            existNewPolicy.getName().equals(policy.getName())
                                || existNewPolicy.getResources().equals(policy.getResources()));
            if (alreadyExist) {
              LOG.warn(
                  "The Ranger policy for the metadata object({}) already exists!",
                  newAuthzMetaObject);
              return;
            }

            // Update the policy
            rangerClient.updatePolicy(policy.getId(), policy);
          } catch (RangerServiceException e) {
            LOG.error("Failed to rename the policy {}!", policy);
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * If remove the SCHEMA, need to remove these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` <br>
   * If remove the TABLE, need to remove these the relevant policies, `{schema}.*`, `{schema}.*.*`
   * <br>
   * If remove the COLUMN, Only need to remove `{schema}.*.*` <br>
   * If remove the PATH, Only need to remove `{path}` <br>
   */
  @Override
  protected void removeMetadataObject(AuthorizationMetadataObject authzMetadataObject) {
    if (authzMetadataObject.metadataObjectType() == MetadataObject.Type.SCHEMA) {
      removeSchemaMetadataObject(authzMetadataObject);
    } else if (authzMetadataObject.metadataObjectType() == MetadataObject.Type.TABLE) {
      removeTableMetadataObject(authzMetadataObject);
    } else if (authzMetadataObject.metadataObjectType() == MetadataObject.Type.FILESET) {
      removePolicyByMetadataObject(authzMetadataObject);
    } else if (authzMetadataObject.metadataObjectType() == MetadataObject.Type.CATALOG) {
      removeCatalogMetadataObject(authzMetadataObject);
    } else if (authzMetadataObject.metadataObjectType() == MetadataObject.Type.METALAKE) {
      // Do nothing
    } else {
      throw new IllegalArgumentException(
          "Unsupported authorization metadata object type: " + authzMetadataObject.type());
    }
  }

  /**
   * Remove the CATALOG, need to remove the Gravitino-managed principals of the policy on the
   * catalog location that are there only for the dropped catalog, and the policies of its schemas
   * and tables. A force drop deletes the schemas and tables without their own drop hooks, so this
   * is the only place their policies are removed. Every location is looked up before any policy
   * changes, so a failed lookup refuses the drop.
   *
   * <p>Another Hive catalog on the same location shares the metastore's schemas and tables, so
   * their policies stay in that case.
   */
  private void removeCatalogMetadataObject(AuthorizationMetadataObject authzMetadataObject) {
    Preconditions.checkArgument(
        authzMetadataObject instanceof PathBasedMetadataObject,
        "The metadata object must be a PathBasedMetadataObject");
    PathBasedMetadataObject pathBasedMetadataObject = (PathBasedMetadataObject) authzMetadataObject;
    String catalogName = pathBasedMetadataObject.name();
    String path = getAuthorizationPath(pathBasedMetadataObject);
    boolean sharedWithHiveCatalog = isPathUsedByOtherCatalog(catalogName, path);
    List<PathBasedMetadataObject> children =
        sharedWithHiveCatalog
            ? Collections.emptyList()
            : loadChildrenMetadataObjects(catalogName, path);
    RangerPolicy policy = findManagedPolicy(authzMetadataObject);
    if (policy != null) {
      removeCatalogPathPrincipals(policy, catalogName, path, sharedWithHiveCatalog);
    }
    children.forEach(this::removeMetadataObject);
  }

  /**
   * Removes the principals of the catalog location policy that are there only for the dropped
   * catalog. Gravitino keeps one Ranger policy per path, so the policy also carries the privileges
   * of other objects on the same path, and a principal stays when it still has a reason to be
   * there:
   *
   * <ul>
   *   <li>A Gravitino role stays when one of its securable objects outside the dropped catalog is
   *       located at the path.
   *   <li>The owner role of another catalog stays.
   *   <li>Metalake-level grants, the metalake owner role, and schema, table or fileset owners stay
   *       when another object uses the path: a kept role, another catalog's owner role, or another
   *       Hive catalog with the same location.
   * </ul>
   *
   * Owners have no securable objects to check, so an owner of another catalog's object on the path,
   * with no grants or catalog there, is removed.
   */
  private void removeCatalogPathPrincipals(
      RangerPolicy policy, String catalogName, String path, boolean sharedWithHiveCatalog) {
    List<List<? extends RangerPolicy.RangerPolicyItem>> itemLists =
        ImmutableList.of(
            policy.getPolicyItems(),
            policy.getDenyPolicyItems(),
            policy.getRowFilterPolicyItems(),
            policy.getDataMaskPolicyItems());
    Set<String> rangerRoles =
        itemLists.stream()
            .flatMap(List::stream)
            .filter(RangerHelper::isGravitinoManagedPolicyItemAccess)
            .flatMap(item -> item.getRoles().stream())
            .filter(
                role ->
                    role.startsWith(RangerHelper.GRAVITINO_ROLE_PREFIX)
                        && !role.equalsIgnoreCase(RangerHelper.GRAVITINO_OWNER_ROLE))
            .collect(Collectors.toSet());

    Set<String> rolesToRemove = new HashSet<>();
    Set<String> rolesKeptIfShared = new HashSet<>();
    boolean pathShared = sharedWithHiveCatalog;
    for (String rangerRole : rangerRoles) {
      switch (catalogPathRoleUse(rangerRole, catalogName, path)) {
        case KEEP:
          pathShared = true;
          break;
        case KEEP_IF_SHARED:
          rolesKeptIfShared.add(rangerRole);
          break;
        default:
          rolesToRemove.add(rangerRole);
          break;
      }
    }
    if (!pathShared) {
      rolesToRemove.addAll(rolesKeptIfShared);
    }

    boolean changed = false;
    for (List<? extends RangerPolicy.RangerPolicyItem> items : itemLists) {
      changed |= removeCatalogPrincipals(items, rolesToRemove, !pathShared);
    }
    if (!changed) {
      LOG.info(
          "Keep the policy of path {} because other objects in metalake {} use it", path, metalake);
      return;
    }
    try {
      if (itemLists.stream().allMatch(List::isEmpty)) {
        rangerClient.deletePolicy(policy.getId());
      } else {
        rangerClient.updatePolicy(policy.getId(), policy);
      }
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(
          e, "Failed to remove the HDFS policy of catalog %s.%s", metalake, catalogName);
    }
  }

  /**
   * Load the path-based metadata objects of the catalog's schemas and tables, whose policies
   * dropping each of them would remove. A schema on the catalog location, usually the default
   * schema, is skipped because it shares the catalog's policy.
   */
  private List<PathBasedMetadataObject> loadChildrenMetadataObjects(
      String catalogName, String catalogPath) {
    List<PathBasedMetadataObject> children = new ArrayList<>();
    for (NameIdentifier schema :
        listChildren(Namespace.of(metalake, catalogName), Entity.EntityType.SCHEMA)) {
      for (String location :
          AuthorizationUtils.loadMetadataObjectLocation(schema, Entity.EntityType.SCHEMA)) {
        PathBasedMetadataObject schemaObject =
            new PathBasedMetadataObject(
                catalogName, schema.name(), location, PathBasedMetadataObject.SCHEMA_PATH);
        if (!StringUtils.removeEnd(getAuthorizationPath(schemaObject), "/")
            .equals(StringUtils.removeEnd(catalogPath, "/"))) {
          children.add(schemaObject);
        }
      }
      for (NameIdentifier table :
          listChildren(
              Namespace.of(metalake, catalogName, schema.name()), Entity.EntityType.TABLE)) {
        for (String location :
            AuthorizationUtils.loadMetadataObjectLocation(table, Entity.EntityType.TABLE)) {
          children.add(
              new PathBasedMetadataObject(
                  catalogName + "." + schema.name(),
                  table.name(),
                  location,
                  PathBasedMetadataObject.TABLE_PATH));
        }
      }
    }
    return children;
  }

  private NameIdentifier[] listChildren(Namespace namespace, Entity.EntityType type) {
    try {
      return type == Entity.EntityType.SCHEMA
          ? GravitinoEnv.getInstance().internalSchemaDispatcher().listSchemas(namespace)
          : GravitinoEnv.getInstance().internalTableDispatcher().listTables(namespace);
    } catch (Exception e) {
      throw new AuthorizationPluginException(
          e,
          "Cannot remove the HDFS policies of catalog %s.%s: failed to list the %ss of %s",
          namespace.level(0),
          namespace.level(1),
          type.name().toLowerCase(Locale.ROOT),
          namespace);
    }
  }

  private boolean isPathUsedByOtherCatalog(String catalogName, String path) {
    String normalizedPath = normalizePath(path);
    // Catalog info comes from the entity store, so other catalogs are not loaded here.
    Catalog[] catalogs =
        GravitinoEnv.getInstance()
            .internalCatalogDispatcher()
            .listCatalogsInfo(Namespace.of(metalake));
    for (Catalog catalog : catalogs) {
      // Only Hive catalogs have catalog-level locations.
      if (catalog.name().equals(catalogName) || !"hive".equals(catalog.provider())) {
        continue;
      }
      List<String> locations;
      try {
        locations =
            AuthorizationUtils.loadMetadataObjectLocation(
                NameIdentifier.of(metalake, catalog.name()), Entity.EntityType.CATALOG);
      } catch (AuthorizationPluginException e) {
        // Without the other catalog's locations, removing the policy could revoke its privileges.
        throw new AuthorizationPluginException(
            e,
            "Cannot remove the HDFS policy of catalog %s.%s: failed to check whether catalog %s"
                + " uses the same path %s",
            metalake,
            catalogName,
            catalog.name(),
            path);
      }
      for (String location : locations) {
        if (normalizePath(location).equals(normalizedPath)) {
          return true;
        }
      }
    }
    return false;
  }

  /** How a Gravitino role on the dropped catalog's path policy is treated. */
  private enum CatalogPathRoleUse {
    /** The role is there only for the dropped catalog. */
    REMOVE,
    /** The role is there for another object on the path. */
    KEEP,
    /** The role covers every catalog, so it stays only when another object uses the path. */
    KEEP_IF_SHARED
  }

  private CatalogPathRoleUse catalogPathRoleUse(
      String rangerRole, String catalogName, String path) {
    String catalogOwnerRolePrefix = RangerHelper.GRAVITINO_CATALOG_OWNER_ROLE + "_";
    if (rangerRole.equalsIgnoreCase(RangerHelper.generateCatalogOwnerRoleName(catalogId))) {
      return CatalogPathRoleUse.REMOVE;
    }
    if (StringUtils.startsWithIgnoreCase(rangerRole, catalogOwnerRolePrefix)) {
      return CatalogPathRoleUse.KEEP;
    }
    // The legacy catalog owner role is shared by all catalogs.
    if (rangerRole.equalsIgnoreCase(RangerHelper.GRAVITINO_CATALOG_OWNER_ROLE)
        || StringUtils.startsWithIgnoreCase(
            rangerRole, RangerHelper.GRAVITINO_METALAKE_OWNER_ROLE)) {
      return CatalogPathRoleUse.KEEP_IF_SHARED;
    }

    Role role = loadGravitinoRole(rangerRole, catalogName);
    if (role == null) {
      return CatalogPathRoleUse.REMOVE;
    }
    String normalizedPath = normalizePath(path);
    boolean grantsOnMetalake = false;
    for (SecurableObject securableObject : role.securableObjects()) {
      if (securableObject.type() == MetadataObject.Type.METALAKE) {
        grantsOnMetalake = true;
        continue;
      }
      if (!LOCATED_TYPES.contains(securableObject.type())) {
        continue;
      }
      NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, securableObject);
      String objectCatalog =
          securableObject.type() == MetadataObject.Type.CATALOG
              ? ident.name()
              : ident.namespace().level(1);
      if (objectCatalog.equals(catalogName)) {
        continue;
      }
      List<String> locations;
      try {
        locations =
            AuthorizationUtils.loadMetadataObjectLocation(
                ident, MetadataObjectUtil.toEntityType(securableObject));
      } catch (AuthorizationPluginException e) {
        // Without the object's locations, removing the role could revoke its privileges.
        throw new AuthorizationPluginException(
            e,
            "Cannot remove the HDFS policy of catalog %s.%s: failed to check whether %s %s of"
                + " role %s uses the same path %s",
            metalake,
            catalogName,
            securableObject.type(),
            securableObject.fullName(),
            role.name(),
            path);
      }
      if (locations.stream()
          .map(RangerAuthorizationHDFSPlugin::normalizePath)
          .anyMatch(normalizedPath::equals)) {
        return CatalogPathRoleUse.KEEP;
      }
    }
    return grantsOnMetalake ? CatalogPathRoleUse.KEEP_IF_SHARED : CatalogPathRoleUse.REMOVE;
  }

  /**
   * Loads the Gravitino role behind a Ranger role name, or null when the role no longer exists. The
   * Ranger name is the Gravitino name with the Gravitino prefix, unless the Gravitino name already
   * starts with it.
   */
  @Nullable
  private Role loadGravitinoRole(String rangerRole, String catalogName) {
    AccessControlDispatcher accessControl =
        GravitinoEnv.getInstance().internalAccessControlDispatcher();
    List<String> candidates =
        ImmutableList.of(
            rangerRole.substring(RangerHelper.GRAVITINO_ROLE_PREFIX.length()), rangerRole);
    for (String roleName : candidates) {
      try {
        Role role = accessControl.getRole(metalake, roleName);
        if (role != null) {
          return role;
        }
      } catch (NoSuchRoleException e) {
        // Try the next candidate name.
      } catch (RuntimeException e) {
        throw new AuthorizationPluginException(
            e,
            "Cannot remove the HDFS policy of catalog %s.%s: failed to load role %s",
            metalake,
            catalogName,
            roleName);
      }
    }
    return null;
  }

  /**
   * Removes the given Gravitino roles from the items, and the owner users and groups too when
   * {@code removeOwners} is true. Items left without principals are removed.
   *
   * @return true if the items changed
   */
  private static boolean removeCatalogPrincipals(
      List<? extends RangerPolicy.RangerPolicyItem> items,
      Set<String> rolesToRemove,
      boolean removeOwners) {
    boolean changed = false;
    Iterator<? extends RangerPolicy.RangerPolicyItem> iterator = items.iterator();
    while (iterator.hasNext()) {
      RangerPolicy.RangerPolicyItem item = iterator.next();
      if (!RangerHelper.isGravitinoManagedPolicyItemAccess(item)) {
        continue;
      }
      changed |= item.getRoles().removeAll(rolesToRemove);
      if (removeOwners && item.getRoles().remove(RangerHelper.GRAVITINO_OWNER_ROLE)) {
        item.getUsers().clear();
        item.getGroups().clear();
        changed = true;
      }
      if (item.getRoles().isEmpty() && item.getUsers().isEmpty() && item.getGroups().isEmpty()) {
        iterator.remove();
        changed = true;
      }
    }
    return changed;
  }

  private static String normalizePath(String location) {
    return StringUtils.removeEnd(HDFS_PATTERN.matcher(location).replaceAll(""), "/");
  }

  /**
   * Remove the SCHEMA, Need to remove these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` permissions.
   */
  private void removeSchemaMetadataObject(AuthorizationMetadataObject authzMetadataObject) {
    Preconditions.checkArgument(
        authzMetadataObject instanceof PathBasedMetadataObject,
        "The metadata object must be a PathBasedMetadataObject");
    Preconditions.checkArgument(
        authzMetadataObject.names().size() == 2, "The metadata object's name size must be 2");
    Preconditions.checkArgument(
        authzMetadataObject.type().equals(PathBasedMetadataObject.SCHEMA_PATH),
        "The metadata object type must be a path");
    removePolicyByMetadataObject(authzMetadataObject);
  }

  /**
   * Remove the TABLE, Need to remove these the relevant policies, `*.{table}`, `*.{table}.{column}`
   * permissions.
   */
  private void removeTableMetadataObject(AuthorizationMetadataObject authzMetadataObject) {
    Preconditions.checkArgument(
        authzMetadataObject instanceof PathBasedMetadataObject,
        "The metadata object must be a PathBasedMetadataObject");
    Preconditions.checkArgument(
        authzMetadataObject.names().size() == 3, "The metadata object's name size must be 3");
    Preconditions.checkArgument(
        authzMetadataObject.type().equals(PathBasedMetadataObject.TABLE_PATH),
        "The metadata object type must be a path");
    removePolicyByMetadataObject(authzMetadataObject);
  }

  @Override
  protected RangerPolicy createPolicyAddResources(AuthorizationMetadataObject metadataObject) {
    Preconditions.checkArgument(
        metadataObject instanceof PathBasedMetadataObject,
        "The metadata object must be a PathBasedMetadataObject");
    PathBasedMetadataObject pathBasedMetadataObject = (PathBasedMetadataObject) metadataObject;
    RangerPolicy policy = new RangerPolicy();
    policy.setService(rangerServiceName);
    policy.setName(getAuthorizationPath(pathBasedMetadataObject));
    RangerPolicy.RangerPolicyResource policyResource =
        new RangerPolicy.RangerPolicyResource(
            getAuthorizationPath(pathBasedMetadataObject),
            false,
            pathBasedMetadataObject.recursive());
    policy.getResources().put(RangerDefines.PolicyResource.PATH.getName(), policyResource);
    return policy;
  }

  @Override
  public AuthorizationSecurableObject generateAuthorizationSecurableObject(
      AuthorizationMetadataObject object, Set<AuthorizationPrivilege> privileges) {
    object.validateAuthorizationMetadataObject();
    Preconditions.checkArgument(
        object instanceof PathBasedMetadataObject, "Object must be a path based metadata object");
    PathBasedMetadataObject metadataObject = (PathBasedMetadataObject) object;
    return new PathBasedSecurableObject(
        metadataObject.parent(),
        metadataObject.name(),
        metadataObject.path(),
        metadataObject.type(),
        metadataObject.recursive(),
        privileges);
  }

  @Override
  public Set<Privilege.Name> allowPrivilegesRule() {
    return ImmutableSet.of(
        Privilege.Name.CREATE_FILESET,
        Privilege.Name.READ_FILESET,
        Privilege.Name.WRITE_FILESET,
        Privilege.Name.CREATE_TABLE,
        Privilege.Name.SELECT_TABLE,
        Privilege.Name.MODIFY_TABLE,
        Privilege.Name.CREATE_SCHEMA,
        Privilege.Name.USE_SCHEMA);
  }

  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return ImmutableSet.of(
        MetadataObject.Type.TABLE,
        MetadataObject.Type.FILESET,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.METALAKE);
  }

  @Override
  public List<AuthorizationSecurableObject> translatePrivilege(SecurableObject securableObject) {
    List<AuthorizationSecurableObject> rangerSecurableObjects = new ArrayList<>();
    NameIdentifier identifier =
        securableObject.type().equals(MetadataObject.Type.METALAKE)
            ? NameIdentifier.of(securableObject.fullName())
            : NameIdentifier.parse(String.join(".", metalake, securableObject.fullName()));
    securableObject.privileges().stream()
        .filter(Objects::nonNull)
        .forEach(
            gravitinoPrivilege -> {
              Set<AuthorizationPrivilege> rangerPrivileges = new HashSet<>();
              // Ignore unsupported privileges
              if (!privilegesMappingRule().containsKey(gravitinoPrivilege.name())) {
                return;
              }
              privilegesMappingRule()
                  .get(gravitinoPrivilege.name())
                  .forEach(
                      rangerPrivilege ->
                          rangerPrivileges.add(
                              new RangerPrivileges.RangerHDFSPrivilegeImpl(
                                  rangerPrivilege, gravitinoPrivilege.condition())));
              switch (gravitinoPrivilege.name()) {
                case USE_CATALOG:
                case CREATE_CATALOG:
                  // When HDFS is used as the Hive storage layer, Hive does not support the
                  // `USE_CATALOG` and `CREATE_CATALOG` privileges. So, we ignore these
                  // in the RangerAuthorizationHDFSPlugin.
                  break;
                case USE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                      extractMetalakeLocations(
                          securableObject,
                          identifier,
                          rangerSecurableObjects,
                          rangerPrivileges,
                          true);
                      break;
                    case CATALOG:
                    case SCHEMA:
                      AuthorizationUtils.getMetadataObjectLocation(
                              identifier, MetadataObjectUtil.toEntityType(securableObject))
                          .forEach(
                              locationPath -> {
                                createPathBasedMetadataObject(
                                    securableObject,
                                    locationPath,
                                    rangerSecurableObjects,
                                    rangerPrivileges,
                                    true);
                              });
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                          gravitinoPrivilege.name(),
                          securableObject.type());
                  }
                  break;
                case CREATE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                      extractMetalakeLocations(
                          securableObject,
                          identifier,
                          rangerSecurableObjects,
                          rangerPrivileges,
                          false);
                      break;
                    case CATALOG:
                      AuthorizationUtils.getMetadataObjectLocation(
                              identifier, MetadataObjectUtil.toEntityType(securableObject))
                          .forEach(
                              locationPath ->
                                  createPathBasedMetadataObject(
                                      securableObject,
                                      locationPath,
                                      rangerSecurableObjects,
                                      rangerPrivileges,
                                      false));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                          gravitinoPrivilege.name(),
                          securableObject.type());
                  }
                  break;
                case SELECT_TABLE:
                case MODIFY_TABLE:
                case READ_FILESET:
                case WRITE_FILESET:
                  if (!gravitinoPrivilege.canBindTo(securableObject.type())) {
                    throw new AuthorizationPluginException(
                        ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                        gravitinoPrivilege.name(),
                        securableObject.type());
                  }
                  createSecurableObjects(
                      securableObject,
                      rangerSecurableObjects,
                      identifier,
                      rangerPrivileges,
                      true,
                      new TableOrFilesetPathExtractor());
                  break;
                case CREATE_TABLE:
                case CREATE_FILESET:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                    case SCHEMA:
                      createSecurableObjects(
                          securableObject,
                          rangerSecurableObjects,
                          identifier,
                          rangerPrivileges,
                          false,
                          new SchemaPathExtractor());

                      break;
                    default:
                      throw new AuthorizationPluginException(
                          ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                          gravitinoPrivilege.name(),
                          securableObject.type());
                  }
                  break;
                default:
                  throw new AuthorizationPluginException(
                      ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                      gravitinoPrivilege.name(),
                      securableObject.type());
              }
            });

    return rangerSecurableObjects;
  }

  private void createSecurableObjects(
      SecurableObject securableObject,
      List<AuthorizationSecurableObject> rangerSecurableObjects,
      NameIdentifier identifier,
      Set<AuthorizationPrivilege> rangerPrivileges,
      boolean recursive,
      PathExtractor pathExtractor) {
    Entity.EntityType type = MetadataObjectUtil.toEntityType(securableObject);
    List<String> locations = Lists.newArrayList();
    if (type == Entity.EntityType.METALAKE) {
      NameIdentifier[] catalogs =
          GravitinoEnv.getInstance()
              .internalCatalogDispatcher()
              .listCatalogs(Namespace.of(identifier.name()));
      for (NameIdentifier catalog : catalogs) {
        locations.addAll(
            AuthorizationUtils.getMetadataObjectLocation(catalog, Entity.EntityType.CATALOG));
      }
    } else {
      locations.addAll(AuthorizationUtils.getMetadataObjectLocation(identifier, type));
    }

    locations.forEach(
        locationPath -> {
          Entity.EntityType pathEntityType;
          MetadataObject.Type pathObjectType;
          if (type == Entity.EntityType.METALAKE) {
            pathEntityType = Entity.EntityType.CATALOG;
            pathObjectType = MetadataObject.Type.CATALOG;
          } else {
            pathEntityType = type;
            pathObjectType = securableObject.type();
          }
          PathBasedMetadataObject pathBaseMetadataObject =
              new PathBasedMetadataObject(
                  securableObject.parent(),
                  securableObject.name(),
                  pathExtractor.getPath(pathEntityType, locationPath),
                  PathBasedMetadataObject.PathType.get(pathObjectType),
                  recursive);
          pathBaseMetadataObject.validateAuthorizationMetadataObject();
          rangerSecurableObjects.add(
              generateAuthorizationSecurableObject(pathBaseMetadataObject, rangerPrivileges));
        });
  }

  private void extractMetalakeLocations(
      SecurableObject securableObject,
      NameIdentifier identifier,
      List<AuthorizationSecurableObject> rangerSecurableObjects,
      Set<AuthorizationPrivilege> rangerPrivileges,
      boolean recursive) {
    NameIdentifier[] catalogs =
        GravitinoEnv.getInstance()
            .internalCatalogDispatcher()
            .listCatalogs(Namespace.of(identifier.name()));
    for (NameIdentifier catalog : catalogs) {
      AuthorizationUtils.getMetadataObjectLocation(catalog, Entity.EntityType.CATALOG)
          .forEach(
              locationPath ->
                  createPathBasedMetadataObject(
                      securableObject,
                      locationPath,
                      rangerSecurableObjects,
                      rangerPrivileges,
                      recursive));
    }
  }

  private void createPathBasedMetadataObject(
      SecurableObject securableObject,
      String locationPath,
      List<AuthorizationSecurableObject> rangerSecurableObjects,
      Set<AuthorizationPrivilege> rangerPrivileges,
      boolean recursive) {
    PathBasedMetadataObject pathBaseMetadataObject =
        new PathBasedMetadataObject(
            securableObject.parent(),
            securableObject.name(),
            locationPath,
            PathBasedMetadataObject.PathType.get(securableObject.type()),
            recursive);
    pathBaseMetadataObject.validateAuthorizationMetadataObject();
    rangerSecurableObjects.add(
        generateAuthorizationSecurableObject(pathBaseMetadataObject, rangerPrivileges));
  }

  @Override
  public List<AuthorizationSecurableObject> translateOwner(MetadataObject gravitinoMetadataObject) {
    List<AuthorizationSecurableObject> rangerSecurableObjects = new ArrayList<>();
    switch (gravitinoMetadataObject.type()) {
      case METALAKE:
      case CATALOG:
      case SCHEMA:
      case FILESET:
      case TABLE:
        translateMetadataObject(gravitinoMetadataObject)
            .forEach(
                metadataObject -> {
                  Preconditions.checkArgument(
                      metadataObject instanceof PathBasedMetadataObject,
                      "The metadata object must be a PathBasedMetadataObject");
                  PathBasedMetadataObject pathBasedMetadataObject =
                      (PathBasedMetadataObject) metadataObject;
                  rangerSecurableObjects.add(
                      generateAuthorizationSecurableObject(
                          pathBasedMetadataObject, ownerMappingRule()));
                });
        break;
      default:
        throw new AuthorizationPluginException(
            ErrorMessages.OWNER_PRIVILEGE_NOT_SUPPORTED, gravitinoMetadataObject.type());
    }

    return rangerSecurableObjects;
  }

  @Override
  public List<AuthorizationMetadataObject> translateMetadataObject(MetadataObject metadataObject) {
    List<AuthorizationMetadataObject> authzMetadataObjects = new ArrayList<>();
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);
    NameIdentifier identifier =
        metadataObject.type().equals(MetadataObject.Type.METALAKE)
            ? NameIdentifier.of(metadataObject.fullName())
            : NameIdentifier.parse(String.join(".", metalake, metadataObject.fullName()));

    List<String> locations = Lists.newArrayList();
    if (metadataObject.type() == MetadataObject.Type.METALAKE) {
      NameIdentifier[] catalogs =
          GravitinoEnv.getInstance()
              .internalCatalogDispatcher()
              .listCatalogs(Namespace.of(identifier.name()));
      for (NameIdentifier catalog : catalogs) {
        locations.addAll(
            AuthorizationUtils.getMetadataObjectLocation(catalog, Entity.EntityType.CATALOG));
      }
    } else {
      locations.addAll(AuthorizationUtils.getMetadataObjectLocation(identifier, entityType));
    }

    locations.forEach(
        locationPath -> {
          AuthorizationMetadataObject.Type type =
              PathBasedMetadataObject.PathType.get(metadataObject.type());
          PathBasedMetadataObject pathBaseMetadataObject =
              new PathBasedMetadataObject(
                  metadataObject.parent(), metadataObject.name(), locationPath, type);
          pathBaseMetadataObject.validateAuthorizationMetadataObject();
          authzMetadataObjects.add(pathBaseMetadataObject);
        });
    return authzMetadataObjects;
  }

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
    for (MetadataObjectChange change : changes) {
      if (change instanceof MetadataObjectChange.RenameMetadataObject) {
        MetadataObjectChange.RenameMetadataObject renameChange =
            (MetadataObjectChange.RenameMetadataObject) change;
        MetadataObject metadataObject = renameChange.metadataObject();
        MetadataObject newMetadataObject = renameChange.newMetadataObject();
        Preconditions.checkArgument(
            metadataObject.type() == newMetadataObject.type(),
            "The old and new metadata object types must be equal!");
        if (metadataObject.type() == MetadataObject.Type.METALAKE) {
          // Rename the metalake name
          this.metalake = newMetadataObject.name();
          // Did not need to update the Ranger policy
          continue;
        } else if (metadataObject.type() == MetadataObject.Type.CATALOG) {
          // Did not need to update the Ranger policy
          continue;
        }

        // Like topics and filesets, their locations don't change, we don't need to modify the
        // policies
        if (renameChange.locations() == null || renameChange.locations().isEmpty()) {
          continue;
        }

        // Only Hive managed tables will change the location
        if (metadataObject.type() == MetadataObject.Type.TABLE) {
          NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, newMetadataObject);
          NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(ident);
          if (GravitinoEnv.getInstance()
              .internalCatalogDispatcher()
              .loadCatalog(catalogIdent)
              .provider()
              .equals("hive")) {
            Table table = GravitinoEnv.getInstance().internalTableDispatcher().loadTable(ident);
            if (table.properties().get("table-type").equals("EXTERNAL_TABLE")) {
              continue;
            }
          } else {
            // Iceberg and other lake houses don't need to change the privileges of locations
            continue;
          }
        }

        List<AuthorizationMetadataObject> oldAuthzMetadataObjects = Lists.newArrayList();
        renameChange
            .locations()
            .forEach(
                locationPath -> {
                  PathBasedMetadataObject pathBaseMetadataObject =
                      new PathBasedMetadataObject(
                          metadataObject.parent(),
                          metadataObject.name(),
                          locationPath,
                          PathBasedMetadataObject.PathType.get(metadataObject.type()));
                  pathBaseMetadataObject.validateAuthorizationMetadataObject();
                  oldAuthzMetadataObjects.add(pathBaseMetadataObject);
                });

        List<AuthorizationMetadataObject> newAuthzMetadataObjects =
            translateMetadataObject(newMetadataObject);
        Preconditions.checkArgument(
            oldAuthzMetadataObjects.size() == newAuthzMetadataObjects.size(),
            "The old and new metadata objects sizes must be equal!");
        for (int i = 0; i < oldAuthzMetadataObjects.size(); i++) {
          AuthorizationMetadataObject oldAuthMetadataObject = oldAuthzMetadataObjects.get(i);
          AuthorizationMetadataObject newAuthzMetadataObject = newAuthzMetadataObjects.get(i);
          if (oldAuthMetadataObject.equals(newAuthzMetadataObject)) {
            LOG.info(
                "The metadata object({}) and new metadata object({}) are equal, so ignoring rename!",
                oldAuthMetadataObject.fullName(),
                newAuthzMetadataObject.fullName());
            continue;
          }
          renameMetadataObject(oldAuthMetadataObject, newAuthzMetadataObject);
        }
      } else if (change instanceof MetadataObjectChange.RemoveMetadataObject) {
        MetadataObjectChange.RemoveMetadataObject changeMetadataObject =
            ((MetadataObjectChange.RemoveMetadataObject) change);
        List<AuthorizationMetadataObject> authzMetadataObjects = new ArrayList<>();
        changeMetadataObject
            .getLocations()
            .forEach(
                locationPath -> {
                  PathBasedMetadataObject pathBaseMetadataObject =
                      new PathBasedMetadataObject(
                          changeMetadataObject.metadataObject().parent(),
                          changeMetadataObject.metadataObject().name(),
                          locationPath,
                          PathBasedMetadataObject.PathType.get(
                              changeMetadataObject.metadataObject().type()));
                  pathBaseMetadataObject.validateAuthorizationMetadataObject();
                  authzMetadataObjects.add(pathBaseMetadataObject);
                });
        authzMetadataObjects.forEach(this::removeMetadataObject);
      } else {
        throw new IllegalArgumentException(
            "Unsupported metadata object change type: "
                + (change == null ? "null" : change.getClass().getSimpleName()));
      }
    }
    return Boolean.TRUE;
  }

  @Override
  protected String getServiceType() {
    return HDFS_SERVICE_TYPE;
  }

  @Override
  protected Map<String, String> getServiceConfigs(Map<String, String> config) {
    return ImmutableMap.<String, String>builder()
        .put(
            RangerAuthorizationProperties.RANGER_USERNAME.substring(getPrefixLength()),
            config.get(RangerAuthorizationProperties.RANGER_USERNAME))
        .put(
            RangerAuthorizationProperties.RANGER_PASSWORD.substring(getPrefixLength()),
            config.get(RangerAuthorizationProperties.RANGER_PASSWORD))
        .put(
            RangerAuthorizationProperties.HADOOP_SECURITY_AUTHENTICATION.substring(
                getPrefixLength()),
            getConfValue(
                config,
                RangerAuthorizationProperties.HADOOP_SECURITY_AUTHENTICATION,
                RangerAuthorizationProperties.DEFAULT_HADOOP_SECURITY_AUTHENTICATION))
        .put(
            RangerAuthorizationProperties.HADOOP_RPC_PROTECTION.substring(getPrefixLength()),
            getConfValue(
                config,
                RangerAuthorizationProperties.HADOOP_RPC_PROTECTION,
                RangerAuthorizationProperties.DEFAULT_HADOOP_RPC_PROTECTION))
        .put(
            RangerAuthorizationProperties.HADOOP_SECURITY_AUTHORIZATION.substring(
                getPrefixLength()),
            getConfValue(
                config, RangerAuthorizationProperties.HADOOP_SECURITY_AUTHORIZATION, "false"))
        .put(
            RangerAuthorizationProperties.FS_DEFAULT_NAME.substring(getPrefixLength()),
            getConfValue(
                config,
                RangerAuthorizationProperties.FS_DEFAULT_NAME,
                RangerAuthorizationProperties.FS_DEFAULT_VALUE))
        .build();
  }

  private interface PathExtractor {
    String getPath(Entity.EntityType type, String location);
  }

  /** The extractor will extra the table or fileset level location path for this entity */
  private static class TableOrFilesetPathExtractor implements PathExtractor {
    /**
     * This method will return table or fileset level path for this entity
     *
     * @param type The entity type
     * @param location The location of this entity
     * @return The table or file locations of this entity
     */
    @Override
    public String getPath(Entity.EntityType type, String location) {
      if (type == Entity.EntityType.CATALOG) {
        return String.format("%s/*/*", location);
      } else if (type == Entity.EntityType.SCHEMA) {
        return String.format("%s/*", location);
      }
      if (type == Entity.EntityType.TABLE || type == Entity.EntityType.FILESET) {
        return location;
      } else {
        throw new AuthorizationPluginException(
            "It's not allowed to extract table or fileset path from entity %s", type);
      }
    }
  }

  /** The extractor will extra the schema level location path for this entity */
  private static class SchemaPathExtractor implements PathExtractor {

    /**
     * This method will return schema level path for this entity
     *
     * @param type The entity type
     * @param location The location of this entity
     * @return The schema locations of this entity
     */
    @Override
    public String getPath(Entity.EntityType type, String location) {
      if (type == Entity.EntityType.CATALOG) {
        return String.format("%s/*/", location);
      } else if (type == Entity.EntityType.SCHEMA) {
        return location;
      } else {
        throw new AuthorizationPluginException(
            "It's not allowed to extract table or fileset path from entity %s", type);
      }
    }
  }
}
