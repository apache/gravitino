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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a helper class for the Ranger authorization plugin. It provides the ability to
 * manage the Ranger policies and roles.
 */
public class RangerHelper {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHelper.class);
  public static final String MANAGED_BY_GRAVITINO = "MANAGED_BY_GRAVITINO";

  RangerAuthorizationPlugin rangerAuthorizationPlugin;

  /** Mapping Gravitino privilege name to the underlying authorization system privileges. */
  protected Map<Privilege.Name, Set<String>> privilegesMapping = null;
  /** The owner privileges, the owner can do anything on the metadata object */
  protected Set<String> ownerPrivileges = null;

  /**
   * Because Ranger doesn't support the precise search, Ranger will return the policy meets the
   * wildcard(*,?) conditions, If you use `db.table` condition to search policy, the Ranger will
   * match `db1.table1`, `db1.table2`, `db*.table*`, So we need to manually precisely filter this
   * research results. <br>
   * policySearchKeys: The search Ranger policy condition key defines. <br>
   * policyPreciseFilterKeys: The precise filter Ranger search results key defines <br>
   */
  protected List<String> policySearchKeys = null;

  protected List<String> policyPreciseFilterKeys = null;

  public RangerHelper(RangerAuthorizationPlugin rangerAuthorizationPlugin, String catalogProvider) {
    this.rangerAuthorizationPlugin = rangerAuthorizationPlugin;
    switch (catalogProvider) {
      case "hive":
        initPrivilegesMapping();
        initOwnerPrivileges();
        initPolicySearchKeys();
        initPreciseFilterKeys();
        break;
      default:
        throw new IllegalArgumentException(
            "Authorization plugin unsupported catalog provider: " + catalogProvider);
    }
  }

  /** Initial mapping Gravitino privilege name to the underlying authorization system privileges. */
  private void initPrivilegesMapping() {
    privilegesMapping =
        ImmutableMap.<Privilege.Name, Set<String>>builder()
            .put(
                Privilege.Name.CREATE_SCHEMA,
                ImmutableSet.of(RangerDefines.ACCESS_TYPE_HIVE_CREATE))
            .put(
                Privilege.Name.CREATE_TABLE, ImmutableSet.of(RangerDefines.ACCESS_TYPE_HIVE_CREATE))
            .put(
                Privilege.Name.MODIFY_TABLE,
                ImmutableSet.of(
                    RangerDefines.ACCESS_TYPE_HIVE_UPDATE,
                    RangerDefines.ACCESS_TYPE_HIVE_ALTER,
                    RangerDefines.ACCESS_TYPE_HIVE_WRITE))
            .put(
                Privilege.Name.SELECT_TABLE,
                ImmutableSet.of(
                    RangerDefines.ACCESS_TYPE_HIVE_READ, RangerDefines.ACCESS_TYPE_HIVE_SELECT))
            .build();
  }

  /** Initial Owner privileges */
  private void initOwnerPrivileges() {
    ownerPrivileges = ImmutableSet.of(RangerDefines.ACCESS_TYPE_HIVE_ALL);
  }

  /** Initial Ranger policy search key defines */
  private void initPolicySearchKeys() {
    policySearchKeys =
        Arrays.asList(
            RangerDefines.SEARCH_FILTER_DATABASE,
            RangerDefines.SEARCH_FILTER_TABLE,
            RangerDefines.SEARCH_FILTER_COLUMN);
  }

  /** Initial precise filter key defines */
  private void initPreciseFilterKeys() {
    policyPreciseFilterKeys =
        Arrays.asList(
            RangerDefines.RESOURCE_DATABASE,
            RangerDefines.RESOURCE_TABLE,
            RangerDefines.RESOURCE_COLUMN);
  }

  /**
   * For easy management, each privilege will create one RangerPolicyItemAccess in the policy.
   *
   * @param policyItem The policy item to check
   * @throws AuthorizationPluginException If the policy item contains more than one access type
   */
  void checkPolicyItemAccess(RangerPolicy.RangerPolicyItem policyItem)
      throws AuthorizationPluginException {
    if (policyItem.getAccesses().size() != 1) {
      throw new AuthorizationPluginException(
          "The access type only have one in the delegate Gravitino management policy");
    }
    Set<String> setAccesses = new HashSet<>();
    policyItem
        .getAccesses()
        .forEach(
            access -> {
              if (setAccesses.contains(access.getType())) {
                throw new AuthorizationPluginException(
                    "Contain duplicate privilege(%s) in the delegate Gravitino management policy ",
                    access.getType());
              }
              setAccesses.add(access.getType());
            });
  }

  /**
   * Add policy item access items base the securable object's privileges. <br>
   * We cannot clean the policy items because one Ranger policy maybe contains multiple Gravitino
   * securable objects. <br>
   */
  void addPolicyItem(RangerPolicy policy, String roleName, SecurableObject securableObject) {
    // First check the privilege if support in the Ranger Hive
    checkPrivileges(securableObject);

    // Add the policy items by the securable object's privileges
    securableObject
        .privileges()
        .forEach(
            gravitinoPrivilege -> {
              // Translate the Gravitino privilege to map Ranger privilege
              rangerAuthorizationPlugin
                  .translatePrivilege(gravitinoPrivilege.name())
                  .forEach(
                      mappedPrivilege -> {
                        // Find the policy item that matches Gravitino privilege
                        List<RangerPolicy.RangerPolicyItem> matchPolicyItems =
                            policy.getPolicyItems().stream()
                                .filter(
                                    policyItem -> {
                                      return policyItem.getAccesses().stream()
                                          .anyMatch(
                                              access -> access.getType().equals(mappedPrivilege));
                                    })
                                .collect(Collectors.toList());

                        if (matchPolicyItems.size() == 0) {
                          // If the policy item does not exist, then create a new policy item
                          RangerPolicy.RangerPolicyItem policyItem =
                              new RangerPolicy.RangerPolicyItem();
                          RangerPolicy.RangerPolicyItemAccess access =
                              new RangerPolicy.RangerPolicyItemAccess();
                          access.setType(mappedPrivilege);
                          policyItem.getAccesses().add(access);
                          policyItem.getRoles().add(roleName);
                          if (Privilege.Condition.ALLOW == gravitinoPrivilege.condition()) {
                            policy.getPolicyItems().add(policyItem);
                          } else {
                            policy.getDenyPolicyItems().add(policyItem);
                          }
                        } else {
                          // If the policy item exists, then add the role to the policy item
                          matchPolicyItems.stream()
                              .forEach(
                                  policyItem -> {
                                    // If the role is not in the policy item, then add it
                                    if (!policyItem.getRoles().contains(roleName)) {
                                      policyItem.getRoles().add(roleName);
                                    }
                                  });
                        }
                      });
            });
  }

  /**
   * Remove policy item base the securable object's privileges and role name. <br>
   * We cannot directly clean the policy items because one Ranger policy maybe contains multiple
   * Gravitino privilege objects. <br>
   */
  void removePolicyItem(RangerPolicy policy, String roleName, SecurableObject securableObject) {
    // First check the privilege if support in the Ranger Hive
    checkPrivileges(securableObject);

    // Delete the policy role base the securable object's privileges
    policy.getPolicyItems().stream()
        .forEach(
            policyItem -> {
              policyItem
                  .getAccesses()
                  .forEach(
                      access -> {
                        boolean matchPrivilege =
                            securableObject.privileges().stream()
                                .filter(Objects::nonNull)
                                .flatMap(
                                    privilege ->
                                        rangerAuthorizationPlugin
                                            .translatePrivilege(privilege.name()).stream())
                                .filter(Objects::nonNull)
                                .anyMatch(
                                    privilege -> {
                                      return access.getType().equals(privilege);
                                    });
                        if (matchPrivilege
                            && !policyItem.getUsers().isEmpty()
                            && !policyItem.getGroups().isEmpty()) {
                          // Not ownership policy item, then remove the role
                          policyItem.getRoles().removeIf(roleName::equals);
                        }
                      });
            });

    // Delete the policy items if the roles are empty and not ownership policy item
    policy
        .getPolicyItems()
        .removeIf(
            policyItem -> {
              return policyItem.getRoles().isEmpty()
                  && policyItem.getUsers().isEmpty()
                  && policyItem.getGroups().isEmpty();
            });
  }

  /**
   * Whether this privilege is underlying permission system supported
   *
   * @param securableObject The securable object to check
   * @return true if the privilege is supported, otherwise false
   */
  private boolean checkPrivileges(SecurableObject securableObject) {
    securableObject
        .privileges()
        .forEach(
            privilege -> {
              check(
                  privilegesMapping.containsKey(privilege.name()),
                  "This privilege %s is not supported in the Ranger hive authorization",
                  privilege.name());
            });
    return true;
  }

  /**
   * Find the managed policy for the metadata object.
   *
   * @param metadataObject The metadata object to find the managed policy.
   * @return The managed policy for the metadata object.
   */
  @VisibleForTesting
  public RangerPolicy findManagedPolicy(MetadataObject metadataObject)
      throws AuthorizationPluginException {
    List<String> nsMetadataObj =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    nsMetadataObj.remove(0); // skip `catalog`
    Map<String, String> policyFilter = new HashMap<>();
    Map<String, String> preciseFilterKeysFilter = new HashMap<>();
    policyFilter.put(
        RangerDefines.SEARCH_FILTER_SERVICE_NAME, rangerAuthorizationPlugin.rangerServiceName);
    policyFilter.put(SearchFilter.POLICY_LABELS_PARTIAL, MANAGED_BY_GRAVITINO);
    for (int i = 0; i < nsMetadataObj.size(); i++) {
      policyFilter.put(policySearchKeys.get(i), nsMetadataObj.get(i));
      preciseFilterKeysFilter.put(policyPreciseFilterKeys.get(i), nsMetadataObj.get(i));
    }

    try {
      List<RangerPolicy> policies =
          rangerAuthorizationPlugin.rangerClient.findPolicies(policyFilter);

      if (!policies.isEmpty()) {
        /**
         * Because Ranger doesn't support the precise search, Ranger will return the policy meets
         * the wildcard(*,?) conditions, If you use `db.table` condition to search policy, the
         * Ranger will match `db1.table1`, `db1.table2`, `db*.table*`, So we need to manually
         * precisely filter this research results.
         */
        policies =
            policies.stream()
                .filter(
                    policy ->
                        policy.getResources().entrySet().stream()
                            .allMatch(
                                entry ->
                                    preciseFilterKeysFilter.containsKey(entry.getKey())
                                        && entry.getValue().getValues().size() == 1
                                        && entry
                                            .getValue()
                                            .getValues()
                                            .contains(preciseFilterKeysFilter.get(entry.getKey()))))
                .collect(Collectors.toList());
      }

      // Only return the policies that are managed by Gravitino.
      if (policies.size() > 1) {
        throw new AuthorizationPluginException(
            "Each metadata object only have one Gravitino management enable policies.");
      }

      RangerPolicy policy = policies.size() == 1 ? policies.get(0) : null;
      // Delegating Gravitino management policies cannot contain duplicate privilege
      if (policy != null) {
        policy.getPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getDenyPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getRowFilterPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getDataMaskPolicyItems().forEach(this::checkPolicyItemAccess);
      }

      return policy;
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e);
    }
  }

  protected boolean checkRangerRole(String roleName) throws AuthorizationPluginException {
    try {
      RangerRole role =
          rangerAuthorizationPlugin.rangerClient.getRole(
              roleName,
              rangerAuthorizationPlugin.rangerAdminName,
              rangerAuthorizationPlugin.rangerServiceName);
      if (role == null) {
        return false;
      }
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e);
    }
    return true;
  }

  protected GrantRevokeRoleRequest createGrantRevokeRoleRequest(
      String roleName, String userName, String groupName) {
    Set<String> users =
        StringUtils.isEmpty(userName) ? Sets.newHashSet() : Sets.newHashSet(userName);
    Set<String> groups =
        StringUtils.isEmpty(groupName) ? Sets.newHashSet() : Sets.newHashSet(groupName);

    GrantRevokeRoleRequest roleRequest = new GrantRevokeRoleRequest();
    roleRequest.setUsers(users);
    roleRequest.setGroups(groups);
    roleRequest.setGrantor(rangerAuthorizationPlugin.rangerAdminName);
    roleRequest.setTargetRoles(Sets.newHashSet(roleName));
    return roleRequest;
  }

  /** Create a Ranger role if the role does not exist. */
  protected RangerRole createRangerRoleIfNotExists(String roleName) {
    RangerRole rangerRole = null;
    try {
      rangerRole =
          rangerAuthorizationPlugin.rangerClient.getRole(
              roleName,
              rangerAuthorizationPlugin.rangerAdminName,
              rangerAuthorizationPlugin.rangerServiceName);
    } catch (RangerServiceException e) {
      // ignore exception, If the role does not exist, then create it.
      LOG.warn("The role({}) is not exist in the Ranger!", roleName);
    }
    try {
      if (rangerRole == null) {
        rangerRole = new RangerRole(roleName, RangerHelper.MANAGED_BY_GRAVITINO, null, null, null);
        rangerAuthorizationPlugin.rangerClient.createRole(
            rangerAuthorizationPlugin.rangerServiceName, rangerRole);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
    return rangerRole;
  }

  protected void updatePolicyOwner(RangerPolicy policy, Owner preOwner, Owner newOwner) {
    // Find matching policy items based on the owner's privileges
    List<RangerPolicy.RangerPolicyItem> matchPolicyItems =
        policy.getPolicyItems().stream()
            .filter(
                policyItem -> {
                  return policyItem.getAccesses().stream()
                      .allMatch(
                          policyItemAccess -> {
                            return ownerPrivileges.contains(policyItemAccess.getType());
                          });
                })
            .collect(Collectors.toList());
    // Add or remove the owner in the policy item
    matchPolicyItems.forEach(
        policyItem -> {
          if (preOwner != null) {
            if (preOwner.type() == Owner.Type.USER) {
              policyItem.getUsers().removeIf(preOwner.name()::equals);
            } else {
              policyItem.getGroups().removeIf(preOwner.name()::equals);
            }
          }
          if (newOwner != null) {
            if (newOwner.type() == Owner.Type.USER) {
              if (!policyItem.getUsers().contains(newOwner.name())) {
                policyItem.getUsers().add(newOwner.name());
              }
            } else {
              if (!policyItem.getGroups().contains(newOwner.name())) {
                policyItem.getGroups().add(newOwner.name());
              }
            }
          }
        });

    // If the policy item is not equal to owner's privileges, then update the policy
    ownerPrivileges.stream()
        .filter(
            ownerPrivilege -> {
              return matchPolicyItems.stream()
                  .noneMatch(
                      policyItem -> {
                        return policyItem.getAccesses().stream()
                            .anyMatch(
                                policyItemAccess -> {
                                  return ownerPrivilege.equals(policyItemAccess.getType());
                                });
                      });
            })
        .forEach(
            // Add lost owner's privilege to the policy
            ownerPrivilege -> {
              RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
              policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege));
              if (newOwner != null) {
                if (newOwner.type() == Owner.Type.USER) {
                  policyItem.getUsers().add(newOwner.name());
                } else {
                  policyItem.getGroups().add(newOwner.name());
                }
              }
              policy.getPolicyItems().add(policyItem);
            });
  }

  protected RangerPolicy addOwnerToNewPolicy(MetadataObject metadataObject, Owner newOwner) {
    RangerPolicy policy = new RangerPolicy();
    policy.setService(rangerAuthorizationPlugin.rangerServiceName);
    policy.setName(metadataObject.fullName());
    policy.setPolicyLabels(Lists.newArrayList(RangerHelper.MANAGED_BY_GRAVITINO));

    List<String> nsMetadataObject =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    if (nsMetadataObject.size() > 4) {
      // The max level of the securable object is `catalog.db.table.column`
      throw new RuntimeException("The securable object than 4");
    }

    List<String> rangerDefinesList =
        Lists.newArrayList(
            RangerDefines.RESOURCE_DATABASE,
            RangerDefines.RESOURCE_TABLE,
            RangerDefines.RESOURCE_COLUMN);
    nsMetadataObject.remove(0); // remove `catalog`
    for (int i = 0; i < nsMetadataObject.size(); i++) {
      RangerPolicy.RangerPolicyResource policyResource =
          new RangerPolicy.RangerPolicyResource(nsMetadataObject.get(i));
      policy.getResources().put(rangerDefinesList.get(i), policyResource);
    }

    ownerPrivileges.stream()
        .forEach(
            ownerPrivilege -> {
              // Each owner's privilege will create one RangerPolicyItemAccess in the policy
              RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
              policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege));
              if (newOwner != null) {
                if (newOwner.type() == Owner.Type.USER) {
                  policyItem.getUsers().add(newOwner.name());
                } else {
                  policyItem.getGroups().add(newOwner.name());
                }
              }
              policy.getPolicyItems().add(policyItem);
            });
    return policy;
  }

  @FormatMethod
  protected static void check(boolean condition, @FormatString String message, Object... args) {
    if (!condition) {
      throw new AuthorizationPluginException(message, args);
    }
  }
}
