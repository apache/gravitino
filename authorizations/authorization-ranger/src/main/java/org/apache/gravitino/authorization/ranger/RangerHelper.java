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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
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
  private final Set<RangerPrivilege> ownerPrivileges;
  /** The policy search keys */
  private final List<String> policyResourceDefines;

  private final RangerClient rangerClient;
  private final String rangerAdminName;
  private final String rangerServiceName;
  public static final String GRAVITINO_METALAKE_OWNER_ROLE = "GRAVITINO_METALAKE_OWNER_ROLE";
  public static final String GRAVITINO_CATALOG_OWNER_ROLE = "GRAVITINO_CATALOG_OWNER_ROLE";

  public RangerHelper(
      RangerClient rangerClient,
      String rangerAdminName,
      String rangerServiceName,
      Set<RangerPrivilege> ownerPrivileges,
      List<String> resourceDefines) {
    this.rangerClient = rangerClient;
    this.rangerAdminName = rangerAdminName;
    this.rangerServiceName = rangerServiceName;
    this.ownerPrivileges = ownerPrivileges;
    this.policyResourceDefines = resourceDefines;
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
  void addPolicyItem(RangerPolicy policy, String roleName, RangerSecurableObject securableObject) {
    // Add the policy items by the securable object's privileges
    securableObject
        .privileges()
        .forEach(
            rangerPrivilege -> {
              // Find the policy item that matches Gravitino privilege
              List<RangerPolicy.RangerPolicyItem> matchPolicyItems =
                  policy.getPolicyItems().stream()
                      .filter(
                          policyItem -> {
                            return policyItem.getAccesses().stream()
                                .anyMatch(
                                    access -> access.getType().equals(rangerPrivilege.getName()));
                          })
                      .collect(Collectors.toList());

              if (matchPolicyItems.isEmpty()) {
                // If the policy item does not exist, then create a new policy item
                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
                RangerPolicy.RangerPolicyItemAccess access =
                    new RangerPolicy.RangerPolicyItemAccess();
                access.setType(rangerPrivilege.getName());
                policyItem.getAccesses().add(access);
                policyItem.getRoles().add(roleName);
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
                          if (!policyItem.getRoles().contains(roleName)) {
                            policyItem.getRoles().add(roleName);
                          }
                        });
              }
            });
  }

  /**
   * Remove policy item base the securable object's privileges and role name. <br>
   * We cannot directly clean the policy items because one Ranger policy maybe contains multiple
   * Gravitino privilege objects. <br>
   */
  void removePolicyItem(
      RangerPolicy policy, String roleName, RangerSecurableObject securableObject) {
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
                                .anyMatch(
                                    privilege -> {
                                      return access.getType().equals(privilege.getName());
                                    });
                        if (matchPrivilege) {
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
   * Find the managed policy for the ranger securable object.
   *
   * @param rangerMetadataObject The ranger securable object to find the managed policy.
   * @return The managed policy for the metadata object.
   */
  public RangerPolicy findManagedPolicy(RangerMetadataObject rangerMetadataObject)
      throws AuthorizationPluginException {
    List<String> nsMetadataObj = rangerMetadataObject.names();

    Map<String, String> searchFilters = new HashMap<>();
    Map<String, String> preciseFilters = new HashMap<>();
    searchFilters.put(SearchFilter.SERVICE_NAME, rangerServiceName);
    searchFilters.put(SearchFilter.POLICY_LABELS_PARTIAL, MANAGED_BY_GRAVITINO);
    for (int i = 0; i < nsMetadataObj.size(); i++) {
      searchFilters.put(
          SearchFilter.RESOURCE_PREFIX + policyResourceDefines.get(i), nsMetadataObj.get(i));
      preciseFilters.put(policyResourceDefines.get(i), nsMetadataObj.get(i));
    }

    try {
      List<RangerPolicy> policies = rangerClient.findPolicies(searchFilters);

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
        throw new AuthorizationPluginException(
            "Every metadata object has only a Gravitino managed policy.");
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
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e);
    }
  }

  protected boolean checkRangerRole(String roleName) throws AuthorizationPluginException {
    try {
      rangerClient.getRole(roleName, rangerAdminName, rangerServiceName);
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
    if (isOwnerRole) {
      Preconditions.checkArgument(
          roleName.equalsIgnoreCase(GRAVITINO_METALAKE_OWNER_ROLE)
              || roleName.equalsIgnoreCase(GRAVITINO_CATALOG_OWNER_ROLE),
          "The role name should be GRAVITINO_METALAKE_OWNER_ROLE or GRAVITINO_CATALOG_OWNER_ROLE");
    } else {
      Preconditions.checkArgument(
          !roleName.equalsIgnoreCase(GRAVITINO_METALAKE_OWNER_ROLE)
              && !roleName.equalsIgnoreCase(GRAVITINO_CATALOG_OWNER_ROLE),
          "The role name should not be GRAVITINO_METALAKE_OWNER_ROLE or GRAVITINO_CATALOG_OWNER_ROLE");
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
                            return ownerPrivileges.stream()
                                .anyMatch(
                                    ownerPrivilege -> {
                                      return ownerPrivilege.equalsTo(policyItemAccess.getType());
                                    });
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
              }
              policy.getPolicyItems().add(policyItem);
            });
  }

  protected RangerPolicy createPolicyAddResources(RangerMetadataObject metadataObject) {
    RangerPolicy policy = new RangerPolicy();
    policy.setService(rangerServiceName);
    policy.setName(metadataObject.fullName());
    policy.setPolicyLabels(Lists.newArrayList(RangerHelper.MANAGED_BY_GRAVITINO));

    List<String> nsMetadataObject = metadataObject.names();

    for (int i = 0; i < nsMetadataObject.size(); i++) {
      RangerPolicy.RangerPolicyResource policyResource =
          new RangerPolicy.RangerPolicyResource(nsMetadataObject.get(i));
      policy.getResources().put(policyResourceDefines.get(i), policyResource);
    }
    return policy;
  }

  protected RangerPolicy addOwnerToNewPolicy(RangerMetadataObject metadataObject, Owner newOwner) {
    RangerPolicy policy = createPolicyAddResources(metadataObject);

    ownerPrivileges.forEach(
        ownerPrivilege -> {
          // Each owner's privilege will create one RangerPolicyItemAccess in the policy
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
          }
          policy.getPolicyItems().add(policyItem);
        });
    return policy;
  }

  protected RangerPolicy addOwnerRoleToNewPolicy(
      RangerMetadataObject metadataObject, String ownerRoleName) {
    RangerPolicy policy = createPolicyAddResources(metadataObject);

    ownerPrivileges.forEach(
        ownerPrivilege -> {
          // Each owner's privilege will create one RangerPolicyItemAccess in the policy
          RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
          policyItem
              .getAccesses()
              .add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege.getName()));
          policyItem.getRoles().add(ownerRoleName);
          policy.getPolicyItems().add(policyItem);
        });
    return policy;
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
          if (!policyItem.getRoles().contains(ownerRoleName)) {
            policyItem.getRoles().add(ownerRoleName);
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
              if (!policyItem.getRoles().contains(ownerRoleName)) {
                policyItem.getRoles().add(ownerRoleName);
              }
              policy.getPolicyItems().add(policyItem);
            });
  }
}
