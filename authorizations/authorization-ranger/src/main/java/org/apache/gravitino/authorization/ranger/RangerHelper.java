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
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerClient;
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
  /** The `*` gives access to all resources */
  public static final String RESOURCE_ALL = "*";
  /** The owner privileges, the owner can do anything on the metadata object */
  private final Set<AuthorizationPrivilege> ownerPrivileges;
  /** The policy search keys */
  protected final List<String> policyResourceDefines;

  private final RangerClient rangerClient;
  private final String rangerAdminName;
  private final String rangerServiceName;

  public static final String GRAVITINO_ROLE_PREFIX = "GRAVITINO_";
  public static final String GRAVITINO_METALAKE_OWNER_ROLE =
      GRAVITINO_ROLE_PREFIX + "METALAKE_OWNER_ROLE";
  public static final String GRAVITINO_CATALOG_OWNER_ROLE =
      GRAVITINO_ROLE_PREFIX + "CATALOG_OWNER_ROLE";
  // marking owner policy items
  public static final String GRAVITINO_OWNER_ROLE = GRAVITINO_ROLE_PREFIX + "OWNER_ROLE";

  public RangerHelper(
      RangerClient rangerClient,
      String rangerAdminName,
      String rangerServiceName,
      Set<AuthorizationPrivilege> ownerPrivileges,
      List<String> resourceDefines) {
    this.rangerClient = rangerClient;
    this.rangerAdminName = rangerAdminName;
    this.rangerServiceName = rangerServiceName;
    this.ownerPrivileges = ownerPrivileges;
    this.policyResourceDefines = resourceDefines;
  }

  /**
   * There are two types of policy items. Gravitino managed policy items: They contain one privilege
   * each. User-defined policy items: They could contain multiple privileges and not be managed and
   * checked by Gravitino.
   *
   * @param policyItem The policy item to check
   * @throws AuthorizationPluginException If the policy item contains more than one access type
   */
  void checkPolicyItemAccess(RangerPolicy.RangerPolicyItem policyItem)
      throws AuthorizationPluginException {
    if (!isGravitinoManagedPolicyItemAccess(policyItem)) {
      return;
    }
    if (policyItem.getAccesses().size() != 1) {
      throw new AuthorizationPluginException(
          "The access type only have one in the delegate Gravitino management policy");
    }
  }

  /**
   * Add policy item access items base the securable object's privileges. <br>
   * We cannot clean the policy items because one Ranger policy maybe contains multiple Gravitino
   * securable objects. <br>
   */
  void addPolicyItem(
      RangerPolicy policy, String roleName, AuthorizationSecurableObject securableObject) {
    // Add the policy items by the securable object's privileges
    securableObject
        .privileges()
        .forEach(
            rangerPrivilege -> {
              // Find the policy item that matches Gravitino privilege
              List<RangerPolicy.RangerPolicyItem> matchPolicyItems;
              if (rangerPrivilege.condition() == Privilege.Condition.ALLOW) {
                matchPolicyItems =
                    policy.getPolicyItems().stream()
                        .filter(
                            policyItem -> {
                              return policyItem.getAccesses().stream()
                                  .anyMatch(
                                      access ->
                                          access.getType().equals(rangerPrivilege.getName())
                                              && isGravitinoManagedPolicyItemAccess(policyItem));
                            })
                        .collect(Collectors.toList());
              } else {
                matchPolicyItems =
                    policy.getDenyPolicyItems().stream()
                        .filter(
                            policyItem -> {
                              return policyItem.getAccesses().stream()
                                  .anyMatch(
                                      access ->
                                          access.getType().equals(rangerPrivilege.getName())
                                              && isGravitinoManagedPolicyItemAccess(policyItem));
                            })
                        .collect(Collectors.toList());
              }

              if (matchPolicyItems.isEmpty()) {
                // If the policy item does not exist, then create a new policy item
                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
                RangerPolicy.RangerPolicyItemAccess access =
                    new RangerPolicy.RangerPolicyItemAccess();
                access.setType(rangerPrivilege.getName());
                policyItem.getAccesses().add(access);
                policyItem.getRoles().add(generateGravitinoRoleName(roleName));
                if (Privilege.Condition.ALLOW == rangerPrivilege.condition()) {
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
                          if (!policyItem
                              .getRoles()
                              .contains(generateGravitinoRoleName(roleName))) {
                            policyItem.getRoles().add(generateGravitinoRoleName(roleName));
                          }
                        });
              }
            });
  }

  /**
   * Find the managed policies for the ranger securable object.
   *
   * @param metadataNames The metadata object names to find the managed policy.
   * @return The managed policy for the metadata object.
   */
  public List<RangerPolicy> wildcardSearchPolies(List<String> metadataNames)
      throws AuthorizationPluginException {
    Map<String, String> searchFilters = new HashMap<>();
    searchFilters.put(SearchFilter.SERVICE_NAME, rangerServiceName);
    for (int i = 0; i < metadataNames.size(); i++) {
      searchFilters.put(
          SearchFilter.RESOURCE_PREFIX + policyResourceDefines.get(i), metadataNames.get(i));
    }

    try {
      List<RangerPolicy> policies = rangerClient.findPolicies(searchFilters);
      return policies;
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e, "Failed to find the policies in the Ranger");
    }
  }

  /**
   * Find the managed policy for the ranger securable object.
   *
   * @param AuthorizationMetadataObject The ranger securable object to find the managed policy.
   * @return The managed policy for the metadata object.
   */
  public RangerPolicy findManagedPolicy(AuthorizationMetadataObject AuthorizationMetadataObject)
      throws AuthorizationPluginException {
    List<RangerPolicy> policies = wildcardSearchPolies(AuthorizationMetadataObject.names());
    if (!policies.isEmpty()) {
      /**
       * Because Ranger doesn't support the precise search, Ranger will return the policy meets the
       * wildcard(*,?) conditions, If you use `db.table` condition to search policy, the Ranger will
       * match `db1.table1`, `db1.table2`, `db*.table*`, So we need to manually precisely filter
       * this research results.
       */
      List<String> nsMetadataObj = AuthorizationMetadataObject.names();
      Map<String, String> preciseFilters = new HashMap<>();
      for (int i = 0; i < nsMetadataObj.size(); i++) {
        preciseFilters.put(policyResourceDefines.get(i), nsMetadataObj.get(i));
      }
      policies =
          policies.stream()
              .filter(
                  policy ->
                      policy.getResources().entrySet().stream()
                          .allMatch(
                              entry ->
                                  preciseFilters.containsKey(entry.getKey())
                                      && entry.getValue().getValues().size() == 1
                                      && entry
                                          .getValue()
                                          .getValues()
                                          .contains(preciseFilters.get(entry.getKey()))))
              .collect(Collectors.toList());
    }
    // Only return the policies that are managed by Gravitino.
    if (policies.size() > 1) {
      throw new AuthorizationPluginException("Each metadata object can have at most one policy.");
    }

    if (policies.isEmpty()) {
      return null;
    }

    RangerPolicy policy = policies.get(0);
    // Delegating Gravitino management policies cannot contain duplicate privilege
    policy.getPolicyItems().forEach(this::checkPolicyItemAccess);
    policy.getDenyPolicyItems().forEach(this::checkPolicyItemAccess);
    policy.getRowFilterPolicyItems().forEach(this::checkPolicyItemAccess);
    policy.getDataMaskPolicyItems().forEach(this::checkPolicyItemAccess);

    return policy;
  }

  public boolean isGravitinoManagedPolicyItemAccess(RangerPolicy.RangerPolicyItem policyItem) {
    return policyItem.getRoles().stream().anyMatch(role -> role.startsWith(GRAVITINO_ROLE_PREFIX));
  }

  public boolean hasGravitinoManagedPolicyItem(RangerPolicy policy) {
    List<RangerPolicy.RangerPolicyItem> policyItems = policy.getPolicyItems();
    policyItems.addAll(policy.getDenyPolicyItems());
    policyItems.addAll(policy.getRowFilterPolicyItems());
    policyItems.addAll(policy.getDataMaskPolicyItems());
    return policyItems.stream().anyMatch(this::isGravitinoManagedPolicyItemAccess);
  }

  public void removeAllGravitinoManagedPolicyItem(RangerPolicy policy) {
    try {
      policy.setPolicyItems(
          policy.getPolicyItems().stream()
              .filter(item -> !isGravitinoManagedPolicyItemAccess(item))
              .collect(Collectors.toList()));
      policy.setDenyPolicyItems(
          policy.getDenyPolicyItems().stream()
              .filter(item -> !isGravitinoManagedPolicyItemAccess(item))
              .collect(Collectors.toList()));
      policy.setRowFilterPolicyItems(
          policy.getRowFilterPolicyItems().stream()
              .filter(item -> !isGravitinoManagedPolicyItemAccess(item))
              .collect(Collectors.toList()));
      policy.setDataMaskPolicyItems(
          policy.getDataMaskPolicyItems().stream()
              .filter(item -> !isGravitinoManagedPolicyItemAccess(item))
              .collect(Collectors.toList()));
      rangerClient.updatePolicy(policy.getId(), policy);
    } catch (RangerServiceException e) {
      LOG.error("Failed to update the policy {}!", policy);
      throw new RuntimeException(e);
    }
  }

  protected boolean checkRangerRole(String roleName) throws AuthorizationPluginException {
    roleName = generateGravitinoRoleName(roleName);
    try {
      rangerClient.getRole(roleName, rangerAdminName, rangerServiceName);
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(
          e, "Failed to check the role(%s) in the Ranger", roleName);
    }
    return true;
  }

  public String generateGravitinoRoleName(String roleName) {
    if (roleName.startsWith(GRAVITINO_ROLE_PREFIX)) {
      return roleName;
    }
    return GRAVITINO_ROLE_PREFIX + roleName;
  }

  protected GrantRevokeRoleRequest createGrantRevokeRoleRequest(
      String roleName, String userName, String groupName) {
    roleName = generateGravitinoRoleName(roleName);
    Set<String> users =
        StringUtils.isEmpty(userName) ? Sets.newHashSet() : Sets.newHashSet(userName);
    Set<String> groups =
        StringUtils.isEmpty(groupName) ? Sets.newHashSet() : Sets.newHashSet(groupName);

    if (users.size() == 0 && groups.size() == 0) {
      throw new AuthorizationPluginException("The user and group cannot be empty!");
    }

    GrantRevokeRoleRequest roleRequest = new GrantRevokeRoleRequest();
    roleRequest.setUsers(users);
    roleRequest.setGroups(groups);
    roleRequest.setGrantor(rangerAdminName);
    roleRequest.setTargetRoles(Sets.newHashSet(roleName));
    return roleRequest;
  }

  /**
   * Create a Ranger role if the role does not exist.
   *
   * @param roleName The role name to create
   * @param isOwnerRole The role is owner role or not
   */
  protected RangerRole createRangerRoleIfNotExists(String roleName, boolean isOwnerRole) {
    roleName = generateGravitinoRoleName(roleName);
    if (isOwnerRole) {
      Preconditions.checkArgument(
          roleName.equalsIgnoreCase(GRAVITINO_METALAKE_OWNER_ROLE)
              || roleName.equalsIgnoreCase(GRAVITINO_CATALOG_OWNER_ROLE)
              || roleName.equalsIgnoreCase(GRAVITINO_OWNER_ROLE),
          String.format(
              "The role name should be %s or %s or %s",
              GRAVITINO_METALAKE_OWNER_ROLE, GRAVITINO_CATALOG_OWNER_ROLE, GRAVITINO_OWNER_ROLE));
    } else {
      Preconditions.checkArgument(
          !roleName.equalsIgnoreCase(GRAVITINO_METALAKE_OWNER_ROLE)
              && !roleName.equalsIgnoreCase(GRAVITINO_CATALOG_OWNER_ROLE)
              && !roleName.equalsIgnoreCase(GRAVITINO_OWNER_ROLE),
          String.format(
              "The role name should not be %s or %s or %s",
              GRAVITINO_METALAKE_OWNER_ROLE, GRAVITINO_CATALOG_OWNER_ROLE, GRAVITINO_OWNER_ROLE));
    }

    RangerRole rangerRole = null;
    try {
      rangerRole = rangerClient.getRole(roleName, rangerAdminName, rangerServiceName);
    } catch (RangerServiceException e) {
      // ignore exception, If the role does not exist, then create it.
      LOG.warn("The role({}) does not exist in the Ranger!", roleName);
    }
    try {
      if (rangerRole == null) {
        rangerRole = new RangerRole(roleName, RangerHelper.MANAGED_BY_GRAVITINO, null, null, null);
        rangerClient.createRole(rangerServiceName, rangerRole);
      }
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(
          e, "Failed to create the role(%s) in the Ranger", roleName);
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
                            return ownerPrivileges.stream()
                                .anyMatch(
                                    ownerPrivilege -> {
                                      return ownerPrivilege.equalsTo(policyItemAccess.getType());
                                    });
                          });
                })
            .filter(this::isGravitinoManagedPolicyItemAccess)
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
                                  return ownerPrivilege.equalsTo(policyItemAccess.getType());
                                });
                      });
            })
        .forEach(
            // Add lost owner's privilege to the policy
            ownerPrivilege -> {
              RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
              policyItem
                  .getAccesses()
                  .add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege.getName()));
              if (newOwner != null) {
                if (newOwner.type() == Owner.Type.USER) {
                  policyItem.getUsers().add(newOwner.name());
                } else {
                  policyItem.getGroups().add(newOwner.name());
                }
                // mark the policy item is created by Gravitino
                addRoleToPolicyItemIfNoExists(policyItem, GRAVITINO_OWNER_ROLE);
              }
              policy.getPolicyItems().add(policyItem);
            });
  }

  protected void updatePolicyOwnerRole(RangerPolicy policy, String ownerRoleName) {
    // Find matching policy items based on the owner's privileges
    List<RangerPolicy.RangerPolicyItem> matchPolicyItems =
        policy.getPolicyItems().stream()
            .filter(
                policyItem -> {
                  return policyItem.getAccesses().stream()
                      .allMatch(
                          policyItemAccess -> {
                            return ownerPrivileges.stream()
                                .anyMatch(
                                    ownerPrivilege -> {
                                      return ownerPrivilege.equalsTo(policyItemAccess.getType());
                                    });
                          });
                })
            .collect(Collectors.toList());
    // Add or remove the owner role in the policy item
    matchPolicyItems.forEach(
        policyItem -> {
          addRoleToPolicyItemIfNoExists(policyItem, ownerRoleName);
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
                                  return ownerPrivilege.equalsTo(policyItemAccess.getType());
                                });
                      });
            })
        .forEach(
            // Add lost owner's privilege to the policy
            ownerPrivilege -> {
              RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
              policyItem
                  .getAccesses()
                  .add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege.getName()));
              addRoleToPolicyItemIfNoExists(policyItem, ownerRoleName);
              policy.getPolicyItems().add(policyItem);
            });
  }

  private void addRoleToPolicyItemIfNoExists(
      RangerPolicy.RangerPolicyItem policyItem, String roleName) {
    String gravitinoRoleName = generateGravitinoRoleName(roleName);
    if (!policyItem.getRoles().contains(gravitinoRoleName)) {
      policyItem.getRoles().add(gravitinoRoleName);
    }
  }
}
