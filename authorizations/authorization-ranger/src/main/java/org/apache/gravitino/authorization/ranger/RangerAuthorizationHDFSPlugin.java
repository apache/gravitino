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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.common.ErrorMessages;
import org.apache.gravitino.authorization.common.PathBasedMetadataObject;
import org.apache.gravitino.authorization.common.PathBasedSecurableObject;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
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
    } else if (authzMetadataObject.metadataObjectType() == MetadataObject.Type.METALAKE
        || authzMetadataObject.metadataObjectType() == MetadataObject.Type.CATALOG) {
      // Do nothing
    } else {
      throw new IllegalArgumentException(
          "Unsupported authorization metadata object type: " + authzMetadataObject.type());
    }
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
              .catalogDispatcher()
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
            .catalogDispatcher()
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
              .catalogDispatcher()
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
              .catalogDispatcher()
              .loadCatalog(catalogIdent)
              .provider()
              .equals("hive")) {
            Table table = GravitinoEnv.getInstance().tableDispatcher().loadTable(ident);
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
