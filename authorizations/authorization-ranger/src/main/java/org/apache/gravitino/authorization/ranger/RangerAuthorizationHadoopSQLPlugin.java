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

import static org.apache.gravitino.authorization.ranger.RangerHadoopSQLMetadataObject.Type.COLUMN;
import static org.apache.gravitino.authorization.ranger.RangerHadoopSQLMetadataObject.Type.SCHEMA;
import static org.apache.gravitino.authorization.ranger.RangerHadoopSQLMetadataObject.Type.TABLE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.common.ErrorMessages;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.RangerPrivileges.RangerHadoopSQLPrivilege;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerAuthorizationHadoopSQLPlugin extends RangerAuthorizationPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(RangerAuthorizationHadoopSQLPlugin.class);

  public RangerAuthorizationHadoopSQLPlugin(String metalake, Map<String, String> config) {
    super(metalake, config);
  }

  @Override
  /** Set the default mapping Gravitino privilege name to the Ranger rule */
  public Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.CREATE_CATALOG,
        ImmutableSet.of(RangerHadoopSQLPrivilege.CREATE),
        Privilege.Name.USE_CATALOG,
        ImmutableSet.of(RangerHadoopSQLPrivilege.SELECT),
        Privilege.Name.CREATE_SCHEMA,
        ImmutableSet.of(RangerHadoopSQLPrivilege.CREATE),
        Privilege.Name.USE_SCHEMA,
        ImmutableSet.of(RangerHadoopSQLPrivilege.SELECT),
        Privilege.Name.CREATE_TABLE,
        ImmutableSet.of(RangerHadoopSQLPrivilege.CREATE),
        Privilege.Name.MODIFY_TABLE,
        ImmutableSet.of(
            RangerHadoopSQLPrivilege.READ,
            RangerHadoopSQLPrivilege.SELECT,
            RangerHadoopSQLPrivilege.UPDATE,
            RangerHadoopSQLPrivilege.ALTER,
            RangerHadoopSQLPrivilege.WRITE),
        Privilege.Name.SELECT_TABLE,
        ImmutableSet.of(RangerHadoopSQLPrivilege.READ, RangerHadoopSQLPrivilege.SELECT));
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
    Map<String, String> preciseFilters = new HashMap<>();
    for (int i = 0; i < nsMetadataObj.size() && i < policyResourceDefinesRule().size(); i++) {
      preciseFilters.put(policyResourceDefinesRule().get(i), nsMetadataObj.get(i));
    }
    return preciseFindPolicy(authzMetadataObject, preciseFilters);
  }

  /** Wildcard search the Ranger policies in the different Ranger service. */
  @Override
  protected List<RangerPolicy> wildcardSearchPolicies(
      AuthorizationMetadataObject authzMetadataObject) {
    List<String> resourceDefines = policyResourceDefinesRule();
    Map<String, String> searchFilters = new HashMap<>();
    searchFilters.put(SearchFilter.SERVICE_NAME, rangerServiceName);
    for (int i = 0; i < authzMetadataObject.names().size() && i < resourceDefines.size(); i++) {
      searchFilters.put(
          SearchFilter.RESOURCE_PREFIX + resourceDefines.get(i),
          authzMetadataObject.names().get(i));
    }

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
   * If rename the COLUMN, Only need to rename `{schema}.*.*` <br>
   */
  @Override
  protected void renameMetadataObject(
      AuthorizationMetadataObject authzMetadataObject,
      AuthorizationMetadataObject newAuthzMetadataObject) {
    List<Pair<String, String>> mappingOldAndNewMetadata;
    if (newAuthzMetadataObject.type().equals(SCHEMA)) {
      // Rename the SCHEMA, Need to rename these the relevant policies, `{schema}`, `{schema}.*`,
      // * `{schema}.*.*`
      mappingOldAndNewMetadata =
          ImmutableList.of(
              Pair.of(authzMetadataObject.names().get(0), newAuthzMetadataObject.names().get(0)),
              Pair.of(RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL),
              Pair.of(RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL));
    } else if (newAuthzMetadataObject.type().equals(TABLE)) {
      // Rename the TABLE, Need to rename these the relevant policies, `{schema}.*`, `{schema}.*.*`
      mappingOldAndNewMetadata =
          ImmutableList.of(
              Pair.of(authzMetadataObject.names().get(0), newAuthzMetadataObject.names().get(0)),
              Pair.of(authzMetadataObject.names().get(1), newAuthzMetadataObject.names().get(1)),
              Pair.of(RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL));
    } else if (newAuthzMetadataObject.type().equals(COLUMN)) {
      // Rename the COLUMN, Only need to rename `{schema}.*.*`
      mappingOldAndNewMetadata =
          ImmutableList.of(
              Pair.of(authzMetadataObject.names().get(0), newAuthzMetadataObject.names().get(0)),
              Pair.of(authzMetadataObject.names().get(1), newAuthzMetadataObject.names().get(1)),
              Pair.of(authzMetadataObject.names().get(2), newAuthzMetadataObject.names().get(2)));
    } else {
      throw new IllegalArgumentException(
          "Unsupported metadata object type: " + authzMetadataObject.type());
    }

    List<String> oldMetadataNames = new ArrayList<>();
    List<String> newMetadataNames = new ArrayList<>();
    for (int index = 0; index < mappingOldAndNewMetadata.size(); index++) {
      oldMetadataNames.add(mappingOldAndNewMetadata.get(index).getKey());
      newMetadataNames.add(mappingOldAndNewMetadata.get(index).getValue());

      AuthorizationMetadataObject.Type type;
      if (index == 0) {
        type = RangerHadoopSQLMetadataObject.Type.SCHEMA;
      } else if (index == 1) {
        type = RangerHadoopSQLMetadataObject.Type.TABLE;
      } else {
        type = RangerHadoopSQLMetadataObject.Type.COLUMN;
      }
      AuthorizationMetadataObject oldHadoopSQLMetadataObject =
          new RangerHadoopSQLMetadataObject(
              AuthorizationMetadataObject.getParentFullName(oldMetadataNames),
              AuthorizationMetadataObject.getLastName(oldMetadataNames),
              type);
      AuthorizationMetadataObject newHadoopSQLMetadataObject =
          new RangerHadoopSQLMetadataObject(
              AuthorizationMetadataObject.getParentFullName(newMetadataNames),
              AuthorizationMetadataObject.getLastName(newMetadataNames),
              type);
      updatePolicyByMetadataObject(
          type.metadataObjectType(), oldHadoopSQLMetadataObject, newHadoopSQLMetadataObject);
    }
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
    Map<MetadataObject.Type, Integer> operationTypeIndex =
        ImmutableMap.of(
            MetadataObject.Type.SCHEMA, 0,
            MetadataObject.Type.TABLE, 1,
            MetadataObject.Type.COLUMN, 2);
    oldPolicies.stream()
        .forEach(
            policy -> {
              try {
                String policyName = policy.getName();
                int index = operationTypeIndex.get(operationType);

                // Update the policy name is following Gravitino's spec
                if (policy
                    .getName()
                    .equals(
                        AuthorizationSecurableObject.DOT_JOINER.join(oldAuthzMetaObject.names()))) {
                  List<String> policyNames =
                      Lists.newArrayList(
                          AuthorizationSecurableObject.DOT_SPLITTER.splitToList(policyName));
                  Preconditions.checkArgument(
                      policyNames.size() >= oldAuthzMetaObject.names().size(),
                      String.format(ErrorMessages.INVALID_POLICY_NAME, policyName));
                  if (policyNames.get(index).equals(RangerHelper.RESOURCE_ALL)) {
                    // Doesn't need to rename the policy `*`
                    return;
                  }
                  policyNames.set(index, newAuthzMetaObject.names().get(index));
                  policy.setName(AuthorizationSecurableObject.DOT_JOINER.join(policyNames));
                }
                // Update the policy resource name to new name
                policy
                    .getResources()
                    .put(
                        policyResourceDefinesRule().get(index),
                        new RangerPolicy.RangerPolicyResource(
                            newAuthzMetaObject.names().get(index)));

                boolean alreadyExist =
                    existNewPolicies.stream()
                        .anyMatch(
                            existNewPolicy ->
                                existNewPolicy.getName().equals(policy.getName())
                                    || existNewPolicy.getResources().equals(policy.getResources()));
                if (alreadyExist) {
                  LOG.warn(
                      "The Ranger policy for the metadata object ({}) already exists!",
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
   */
  @Override
  protected void removeMetadataObject(AuthorizationMetadataObject authzMetadataObject) {
    AuthorizationMetadataObject.Type type = authzMetadataObject.type();
    if (type.equals(SCHEMA)) {
      doRemoveSchemaMetadataObject(authzMetadataObject);
    } else if (type.equals(TABLE)) {
      doRemoveTableMetadataObject(authzMetadataObject);
    } else if (type.equals(COLUMN)) {
      removePolicyByMetadataObject(authzMetadataObject);
    } else {
      throw new IllegalArgumentException(
          "Unsupported metadata object type: " + authzMetadataObject.type());
    }
  }

  /**
   * Remove the SCHEMA, Need to remove these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` permissions.
   */
  private void doRemoveSchemaMetadataObject(AuthorizationMetadataObject authzMetadataObject) {
    Preconditions.checkArgument(
        authzMetadataObject.type() == SCHEMA, "The metadata object type must be a schema");
    Preconditions.checkArgument(
        authzMetadataObject.names().size() == 1, "The metadata object's name size must be 1");
    if (RangerHelper.RESOURCE_ALL.equals(authzMetadataObject.name())) {
      // Delete metalake or catalog policies in this Ranger service
      try {
        List<RangerPolicy> policies = rangerClient.getPoliciesInService(rangerServiceName);
        policies.stream()
            .filter(RangerHelper::hasGravitinoManagedPolicyItem)
            .forEach(rangerHelper::removeAllGravitinoManagedPolicyItem);
      } catch (RangerServiceException e) {
        throw new RuntimeException(e);
      }
    } else {
      List<Pair<AuthorizationMetadataObject.Type, List<String>>> loop =
          ImmutableList.of(
              Pair.of(
                  RangerHadoopSQLMetadataObject.Type.SCHEMA,
                  ImmutableList.of(authzMetadataObject.name())),
              /** SCHEMA permission */
              Pair.of(
                  RangerHadoopSQLMetadataObject.Type.TABLE,
                  ImmutableList.of(authzMetadataObject.name(), RangerHelper.RESOURCE_ALL)),
              /** TABLE permission */
              Pair.of(
                  RangerHadoopSQLMetadataObject.Type.COLUMN,
                  ImmutableList.of(
                      authzMetadataObject.name(),
                      RangerHelper.RESOURCE_ALL,
                      RangerHelper.RESOURCE_ALL))
              /** COLUMN permission */
              );

      for (int index = 0; index < loop.size(); index++) {
        AuthorizationMetadataObject authzMetadataObject1 =
            new RangerHadoopSQLMetadataObject(
                AuthorizationMetadataObject.getParentFullName(loop.get(index).getValue()),
                AuthorizationMetadataObject.getLastName(loop.get(index).getValue()),
                loop.get(index).getKey());
        removePolicyByMetadataObject(authzMetadataObject1);
      }
    }
  }

  /**
   * Remove the TABLE, Need to remove these the relevant policies, `*.{table}`, `*.{table}.{column}`
   * permissions.
   */
  private void doRemoveTableMetadataObject(AuthorizationMetadataObject authzMetadataObject) {
    List<Pair<AuthorizationMetadataObject.Type, List<String>>> loop =
        ImmutableList.of(
            Pair.of(RangerHadoopSQLMetadataObject.Type.TABLE, authzMetadataObject.names()),
            /** TABLE permission */
            Pair.of(
                RangerHadoopSQLMetadataObject.Type.COLUMN,
                Stream.concat(
                        authzMetadataObject.names().stream(), Stream.of(RangerHelper.RESOURCE_ALL))
                    .collect(Collectors.toList()))
            /** COLUMN permission */
            );

    for (int index = 0; index < loop.size(); index++) {
      AuthorizationMetadataObject authzMetadataObject1 =
          new RangerHadoopSQLMetadataObject(
              AuthorizationMetadataObject.getParentFullName(loop.get(index).getValue()),
              AuthorizationMetadataObject.getLastName(loop.get(index).getValue()),
              loop.get(index).getKey());
      removePolicyByMetadataObject(authzMetadataObject1);
    }
  }

  @Override
  /** Set the default owner rule. */
  public Set<AuthorizationPrivilege> ownerMappingRule() {
    return ImmutableSet.of(RangerHadoopSQLPrivilege.ALL);
  }

  @Override
  /** Set Ranger policy resource rule. */
  public List<String> policyResourceDefinesRule() {
    return ImmutableList.of(
        PolicyResource.DATABASE.getName(),
        PolicyResource.TABLE.getName(),
        PolicyResource.COLUMN.getName());
  }

  @Override
  protected RangerPolicy createPolicyAddResources(AuthorizationMetadataObject metadataObject) {
    RangerPolicy policy = new RangerPolicy();
    policy.setService(rangerServiceName);
    policy.setName(metadataObject.fullName());
    List<String> nsMetadataObject = metadataObject.names();
    for (int i = 0; i < nsMetadataObject.size(); i++) {
      RangerPolicy.RangerPolicyResource policyResource =
          new RangerPolicy.RangerPolicyResource(nsMetadataObject.get(i));
      policy.getResources().put(policyResourceDefinesRule().get(i), policyResource);
    }
    return policy;
  }

  public AuthorizationSecurableObject generateAuthorizationSecurableObject(
      List<String> names,
      AuthorizationMetadataObject.Type type,
      Set<AuthorizationPrivilege> privileges) {
    RangerHadoopSQLMetadataObject object =
        new RangerHadoopSQLMetadataObject(
            AuthorizationMetadataObject.getParentFullName(names),
            AuthorizationMetadataObject.getLastName(names),
            type);
    return generateAuthorizationSecurableObject(object, privileges);
  }

  @Override
  public AuthorizationSecurableObject generateAuthorizationSecurableObject(
      AuthorizationMetadataObject object, Set<AuthorizationPrivilege> privileges) {
    object.validateAuthorizationMetadataObject();
    return new RangerHadoopSQLSecurableObject(
        object.parent(), object.name(), object.type(), privileges);
  }

  @Override
  /** Allow privilege operation defines rule. */
  public Set<Privilege.Name> allowPrivilegesRule() {
    return ImmutableSet.of(
        Privilege.Name.CREATE_CATALOG,
        Privilege.Name.USE_CATALOG,
        Privilege.Name.CREATE_SCHEMA,
        Privilege.Name.USE_SCHEMA,
        Privilege.Name.CREATE_TABLE,
        Privilege.Name.MODIFY_TABLE,
        Privilege.Name.SELECT_TABLE);
  }

  /**
   * Allow Gravitino MetadataObject type defines rule.
   *
   * @return To allow Gravitino MetadataObject type defines rule.
   */
  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return ImmutableSet.of(
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
        MetadataObject.Type.COLUMN);
  }

  /** Translate the Gravitino securable object to the Ranger owner securable object. */
  @Override
  public List<AuthorizationSecurableObject> translateOwner(MetadataObject gravitinoMetadataObject) {
    List<AuthorizationSecurableObject> rangerSecurableObjects = new ArrayList<>();

    switch (gravitinoMetadataObject.type()) {
      case METALAKE:
      case CATALOG:
        // Add `*` for the SCHEMA permission
        rangerSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(RangerHelper.RESOURCE_ALL),
                RangerHadoopSQLMetadataObject.Type.SCHEMA,
                ownerMappingRule()));
        // Add `*.*` for the TABLE permission
        rangerSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL),
                RangerHadoopSQLMetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `*.*.*` for the COLUMN permission
        rangerSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL),
                RangerHadoopSQLMetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      case SCHEMA:
        // Add `{schema}` for the SCHEMA permission
        rangerSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(gravitinoMetadataObject.name() /*Schema name*/),
                RangerHadoopSQLMetadataObject.Type.SCHEMA,
                ownerMappingRule()));
        // Add `{schema}.*` for the TABLE permission
        rangerSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(
                    gravitinoMetadataObject.name() /*Schema name*/, RangerHelper.RESOURCE_ALL),
                RangerHadoopSQLMetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `{schema}.*.*` for the COLUMN permission
        rangerSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(
                    gravitinoMetadataObject.name() /*Schema name*/,
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL),
                RangerHadoopSQLMetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      case TABLE:
        translateMetadataObject(gravitinoMetadataObject).stream()
            .forEach(
                rangerMetadataObject -> {
                  // Add `{schema}.{table}` for the TABLE permission
                  rangerSecurableObjects.add(
                      generateAuthorizationSecurableObject(
                          rangerMetadataObject.names(),
                          RangerHadoopSQLMetadataObject.Type.TABLE,
                          ownerMappingRule()));
                  // Add `{schema}.{table}.*` for the COLUMN permission
                  rangerSecurableObjects.add(
                      generateAuthorizationSecurableObject(
                          Stream.concat(
                                  rangerMetadataObject.names().stream(),
                                  Stream.of(RangerHelper.RESOURCE_ALL))
                              .collect(Collectors.toList()),
                          RangerHadoopSQLMetadataObject.Type.COLUMN,
                          ownerMappingRule()));
                });
        break;
      default:
        throw new AuthorizationPluginException(
            ErrorMessages.OWNER_PRIVILEGE_NOT_SUPPORTED, gravitinoMetadataObject.type());
    }

    return rangerSecurableObjects;
  }

  /** Translate the Gravitino securable object to the Ranger securable object. */
  @Override
  public List<AuthorizationSecurableObject> translatePrivilege(SecurableObject securableObject) {
    List<AuthorizationSecurableObject> rangerSecurableObjects = new ArrayList<>();

    securableObject.privileges().stream()
        .filter(Objects::nonNull)
        .forEach(
            gravitinoPrivilege -> {
              Set<AuthorizationPrivilege> rangerPrivileges = new HashSet<>();
              // Ignore unsupported privileges
              if (!privilegesMappingRule().containsKey(gravitinoPrivilege.name())) {
                return;
              }
              privilegesMappingRule().get(gravitinoPrivilege.name()).stream()
                  .forEach(
                      rangerPrivilege ->
                          rangerPrivileges.add(
                              new RangerPrivileges.RangerHivePrivilegeImpl(
                                  rangerPrivilege, gravitinoPrivilege.condition())));

              switch (gravitinoPrivilege.name()) {
                case CREATE_CATALOG:
                  // Ignore the Gravitino privilege `CREATE_CATALOG` in the
                  // RangerAuthorizationHivePlugin
                  break;
                case USE_CATALOG:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`*`)
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerHadoopSQLMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
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
                    case CATALOG:
                      // Add Ranger privilege(`CREATE`) to SCHEMA(`*`)
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerHadoopSQLMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                          gravitinoPrivilege.name(),
                          securableObject.type());
                  }
                  break;
                case USE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`*`)
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerHadoopSQLMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    case SCHEMA:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`{schema}`)
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(securableObject.name() /*Schema name*/),
                              RangerHadoopSQLMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                          gravitinoPrivilege.name(),
                          securableObject.type());
                  }
                  break;
                case CREATE_TABLE:
                case MODIFY_TABLE:
                case SELECT_TABLE:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add `*.*` for the TABLE permission
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(
                                  RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL),
                              RangerHadoopSQLMetadataObject.Type.TABLE,
                              rangerPrivileges));
                      // Add `*.*.*` for the COLUMN permission
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL),
                              RangerHadoopSQLMetadataObject.Type.COLUMN,
                              rangerPrivileges));
                      break;
                    case SCHEMA:
                      // Add `{schema}.*` for the TABLE permission
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(
                                  securableObject.name() /*Schema name*/,
                                  RangerHelper.RESOURCE_ALL),
                              RangerHadoopSQLMetadataObject.Type.TABLE,
                              rangerPrivileges));
                      // Add `{schema}.*.*` for the COLUMN permission
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              ImmutableList.of(
                                  securableObject.name() /*Schema name*/,
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL),
                              RangerHadoopSQLMetadataObject.Type.COLUMN,
                              rangerPrivileges));
                      break;
                    case TABLE:
                      if (gravitinoPrivilege.name() == Privilege.Name.CREATE_TABLE) {
                        throw new AuthorizationPluginException(
                            ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                            gravitinoPrivilege.name(),
                            securableObject.type());
                      } else {
                        translateMetadataObject(securableObject).stream()
                            .forEach(
                                rangerMetadataObject -> {
                                  // Add `{schema}.{table}` for the TABLE permission
                                  rangerSecurableObjects.add(
                                      generateAuthorizationSecurableObject(
                                          rangerMetadataObject.names(),
                                          RangerHadoopSQLMetadataObject.Type.TABLE,
                                          rangerPrivileges));
                                  // Add `{schema}.{table}.*` for the COLUMN permission
                                  rangerSecurableObjects.add(
                                      generateAuthorizationSecurableObject(
                                          Stream.concat(
                                                  rangerMetadataObject.names().stream(),
                                                  Stream.of(RangerHelper.RESOURCE_ALL))
                                              .collect(Collectors.toList()),
                                          RangerHadoopSQLMetadataObject.Type.COLUMN,
                                          rangerPrivileges));
                                });
                      }
                      break;
                    default:
                      LOG.warn(
                          ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                          gravitinoPrivilege.name(),
                          securableObject.type());
                  }
                  break;
                default:
                  LOG.warn(
                      ErrorMessages.PRIVILEGE_NOT_SUPPORTED,
                      gravitinoPrivilege.name(),
                      securableObject.type());
              }
            });

    return rangerSecurableObjects;
  }

  /**
   * Because the Ranger metadata object is different from the Gravitino metadata object, we need to
   * convert the Gravitino metadata object to the Ranger metadata object.
   */
  @Override
  public List<AuthorizationMetadataObject> translateMetadataObject(MetadataObject metadataObject) {
    Preconditions.checkArgument(
        !(metadataObject instanceof RangerPrivileges),
        "The metadata object must not be a RangerPrivileges object.");
    List<String> nsMetadataObject =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    Preconditions.checkArgument(
        nsMetadataObject.size() > 0, "The metadata object must have at least one name.");

    AuthorizationMetadataObject.Type type;
    if (metadataObject.type() == MetadataObject.Type.METALAKE
        || metadataObject.type() == MetadataObject.Type.CATALOG) {
      nsMetadataObject.clear();
      nsMetadataObject.add(RangerHelper.RESOURCE_ALL);
      type = RangerHadoopSQLMetadataObject.Type.SCHEMA;
    } else {
      nsMetadataObject.remove(0); // Remove the catalog name
      type = RangerHadoopSQLMetadataObject.Type.fromMetadataType(metadataObject.type());
    }

    RangerHadoopSQLMetadataObject rangerHadoopSQLMetadataObject =
        new RangerHadoopSQLMetadataObject(
            AuthorizationMetadataObject.getParentFullName(nsMetadataObject),
            AuthorizationMetadataObject.getLastName(nsMetadataObject),
            type);
    rangerHadoopSQLMetadataObject.validateAuthorizationMetadataObject();
    return ImmutableList.of(rangerHadoopSQLMetadataObject);
  }

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
    for (MetadataObjectChange change : changes) {
      if (change instanceof MetadataObjectChange.RenameMetadataObject) {
        MetadataObject metadataObject =
            ((MetadataObjectChange.RenameMetadataObject) change).metadataObject();
        MetadataObject newMetadataObject =
            ((MetadataObjectChange.RenameMetadataObject) change).newMetadataObject();
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
        List<AuthorizationMetadataObject> oldAuthzMetadataObjects =
            translateMetadataObject(metadataObject);
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
        MetadataObject metadataObject =
            ((MetadataObjectChange.RemoveMetadataObject) change).metadataObject();
        List<AuthorizationMetadataObject> authzMetadataObjects =
            translateMetadataObject(metadataObject);
        authzMetadataObjects.stream().forEach(this::removeMetadataObject);
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
    return HADOOP_SQL_SERVICE_TYPE;
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
            RangerAuthorizationProperties.JDBC_DRIVER_CLASS_NAME.substring(getPrefixLength()),
            getConfValue(
                config,
                RangerAuthorizationProperties.JDBC_DRIVER_CLASS_NAME,
                RangerAuthorizationProperties.DEFAULT_JDBC_DRIVER_CLASS_NAME))
        .put(
            RangerAuthorizationProperties.JDBC_URL.substring(getPrefixLength()),
            getConfValue(
                config,
                RangerAuthorizationProperties.JDBC_URL,
                RangerAuthorizationProperties.DEFAULT_JDBC_URL))
        .build();
  }
}
