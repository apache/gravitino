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
package org.apache.gravitino.authorization.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AuthorizationPlugin} implementation that pushes Apache Gravitino RBAC grants, revocations,
 * ownership changes, and topic metadata updates to Kafka brokers using {@link AdminClient}.
 */
public class KafkaAuthorizationPlugin implements AuthorizationPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAuthorizationPlugin.class);
  private static final String GROUP_PROBE_PRINCIPAL = "Group:__gravitino_group_probe__";

  private final Object aclSyncLock = new Object();
  private final KafkaAuthorizationProperties properties;
  private final AdminClient adminClient;
  private final KafkaAuthorizationStateProvider stateProvider;

  private volatile String metalake;
  private volatile String catalogName;

  /**
   * Creates a {@link KafkaAuthorizationPlugin} backed by a new Kafka {@link AdminClient} and the
   * default {@link EntityStoreAuthorizationStateProvider}.
   *
   * @param metalake Metalake name associated with this catalog plugin.
   * @param properties Validated Kafka authorization configuration properties.
   */
  public KafkaAuthorizationPlugin(String metalake, KafkaAuthorizationProperties properties) {
    this(
        metalake,
        properties,
        AdminClient.create(properties.toAdminClientProperties()),
        new EntityStoreAuthorizationStateProvider(),
        null);
  }

  @VisibleForTesting
  KafkaAuthorizationPlugin(
      String metalake,
      KafkaAuthorizationProperties properties,
      AdminClient adminClient,
      KafkaAuthorizationStateProvider stateProvider,
      @Nullable String initialCatalogName) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake), "Metalake name must not be blank");
    Preconditions.checkArgument(properties != null, "Properties must not be null");
    Preconditions.checkArgument(adminClient != null, "AdminClient must not be null");
    Preconditions.checkArgument(stateProvider != null, "StateProvider must not be null");
    this.metalake = metalake;
    this.properties = properties;
    this.adminClient = adminClient;
    this.stateProvider = stateProvider;
    this.catalogName = initialCatalogName;
  }

  @Override
  public Boolean onRoleCreated(Role role) throws RuntimeException {
    // Roles carry no Kafka user/group principals until granted to a user or group.
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws RuntimeException {
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleDeleted(Role role) throws RuntimeException {
    if (role == null) {
      return Boolean.TRUE;
    }

    synchronized (aclSyncLock) {
      Set<String> excludedRoles = Collections.singleton(role.name());
      List<String> boundUsers = stateProvider.listUserNamesByRole(metalake, role.name());
      for (String username : boundUsers) {
        String principal = properties.formatUserPrincipal(username);
        Set<AclBinding> candidateRemovals = bindingsForRoles(List.of(role), principal);
        removeBindingsPreservingActiveGrants(
            Owner.Type.USER,
            username,
            candidateRemovals,
            excludedRoles,
            excludedRoles,
            Collections.emptyMap(),
            null);
      }

      if (properties.isGroupAclEnabled()) {
        List<String> boundGroups = stateProvider.listGroupNamesByRole(metalake, role.name());
        for (String groupName : boundGroups) {
          String principal = properties.formatGroupPrincipal(groupName);
          Set<AclBinding> candidateRemovals = bindingsForRoles(List.of(role), principal);
          removeBindingsPreservingActiveGrants(
              Owner.Type.GROUP,
              groupName,
              candidateRemovals,
              excludedRoles,
              excludedRoles,
              Collections.emptyMap(),
              null);
        }
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes) throws RuntimeException {
    if (role == null || changes == null || changes.length == 0) {
      return Boolean.TRUE;
    }

    synchronized (aclSyncLock) {
      List<String> boundUsers = stateProvider.listUserNamesByRole(metalake, role.name());
      List<String> boundGroups = stateProvider.listGroupNamesByRole(metalake, role.name());
      if (boundUsers.isEmpty() && boundGroups.isEmpty()) {
        return Boolean.TRUE;
      }

      if (!boundGroups.isEmpty() && !properties.isGroupAclEnabled()) {
        RoleChangeBindings probeDelta =
            computeRoleChangeBindings(role, changes, GROUP_PROBE_PRINCIPAL);
        if (!probeDelta.beforeBindings.isEmpty() || !probeDelta.afterBindings.isEmpty()) {
          ensureGroupAclEnabled();
        }
      }

      for (String username : boundUsers) {
        String principal = properties.formatUserPrincipal(username);
        applyRoleUpdateForPrincipal(Owner.Type.USER, username, principal, role, changes);
      }

      if (properties.isGroupAclEnabled()) {
        for (String groupName : boundGroups) {
          String principal = properties.formatGroupPrincipal(groupName);
          applyRoleUpdateForPrincipal(Owner.Type.GROUP, groupName, principal, role, changes);
        }
      }
    }

    return Boolean.TRUE;
  }

  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user) throws RuntimeException {
    if (user == null || roles == null || roles.isEmpty()) {
      return Boolean.TRUE;
    }
    synchronized (aclSyncLock) {
      String principal = properties.formatUserPrincipal(user.name());
      createAcls(bindingsForRoles(roles, principal));
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws RuntimeException {
    if (user == null || roles == null || roles.isEmpty()) {
      return Boolean.TRUE;
    }
    synchronized (aclSyncLock) {
      String principal = properties.formatUserPrincipal(user.name());
      Set<AclBinding> candidateRemovals = bindingsForRoles(roles, principal);
      removeBindingsPreservingActiveGrants(
          Owner.Type.USER,
          user.name(),
          candidateRemovals,
          extractRoleNames(roles),
          Collections.emptySet(),
          Collections.emptyMap(),
          null);
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws RuntimeException {
    if (group == null || roles == null || roles.isEmpty()) {
      return Boolean.TRUE;
    }
    synchronized (aclSyncLock) {
      if (!properties.isGroupAclEnabled()) {
        if (!bindingsForRoles(roles, GROUP_PROBE_PRINCIPAL).isEmpty()) {
          ensureGroupAclEnabled();
        }
        return Boolean.TRUE;
      }
      String principal = properties.formatGroupPrincipal(group.name());
      createAcls(bindingsForRoles(roles, principal));
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) throws RuntimeException {
    if (group == null || roles == null || roles.isEmpty()) {
      return Boolean.TRUE;
    }
    synchronized (aclSyncLock) {
      if (!properties.isGroupAclEnabled()) {
        if (!bindingsForRoles(roles, GROUP_PROBE_PRINCIPAL).isEmpty()) {
          ensureGroupAclEnabled();
        }
        return Boolean.TRUE;
      }
      String principal = properties.formatGroupPrincipal(group.name());
      Set<AclBinding> candidateRemovals = bindingsForRoles(roles, principal);
      removeBindingsPreservingActiveGrants(
          Owner.Type.GROUP,
          group.name(),
          candidateRemovals,
          extractRoleNames(roles),
          Collections.emptySet(),
          Collections.emptyMap(),
          null);
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAdded(User user) throws RuntimeException {
    // External SASL/OAuth IdP manages user existence; Kafka ACLs are added on role/owner grants.
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserRemoved(User user) throws RuntimeException {
    if (user == null || isWildcardIdentityName(user.name())) {
      return Boolean.TRUE;
    }
    synchronized (aclSyncLock) {
      String principal = properties.formatUserPrincipal(user.name());
      Set<AclBinding> candidateRemovals =
          collectActiveAclBindingsForPrincipal(
              Owner.Type.USER,
              user.name(),
              principal,
              Collections.emptySet(),
              Collections.emptyMap(),
              null);
      if (candidateRemovals.isEmpty()) {
        return Boolean.TRUE;
      }
      if (sameUserAndGroupPrincipalType() && properties.isGroupAclEnabled()) {
        Set<AclBinding> peerBindings =
            collectActiveAclBindingsForPrincipal(
                Owner.Type.GROUP,
                user.name(),
                principal,
                Collections.emptySet(),
                Collections.emptyMap(),
                null);
        candidateRemovals = new LinkedHashSet<>(Sets.difference(candidateRemovals, peerBindings));
      }
      deleteExactAcls(candidateRemovals);
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAcquired(User user) throws RuntimeException {
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupAdded(Group group) throws RuntimeException {
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws RuntimeException {
    if (group == null || !properties.isGroupAclEnabled() || isWildcardIdentityName(group.name())) {
      return Boolean.TRUE;
    }
    synchronized (aclSyncLock) {
      String principal = properties.formatGroupPrincipal(group.name());
      Set<AclBinding> candidateRemovals =
          collectActiveAclBindingsForPrincipal(
              Owner.Type.GROUP,
              group.name(),
              principal,
              Collections.emptySet(),
              Collections.emptyMap(),
              null);
      if (candidateRemovals.isEmpty()) {
        return Boolean.TRUE;
      }
      if (sameUserAndGroupPrincipalType()) {
        Set<AclBinding> peerBindings =
            collectActiveAclBindingsForPrincipal(
                Owner.Type.USER,
                group.name(),
                principal,
                Collections.emptySet(),
                Collections.emptyMap(),
                null);
        candidateRemovals = new LinkedHashSet<>(Sets.difference(candidateRemovals, peerBindings));
      }
      deleteExactAcls(candidateRemovals);
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupAcquired(Group group) throws RuntimeException {
    return Boolean.TRUE;
  }

  @Override
  public Boolean onOwnerSet(
      MetadataObject metadataObject, @Nullable Owner preOwner, @Nullable Owner newOwner)
      throws RuntimeException {
    if (metadataObject == null) {
      return Boolean.TRUE;
    }

    synchronized (aclSyncLock) {
      if (!appliesToThisCatalog(metadataObject)) {
        return Boolean.TRUE;
      }

      boolean groupTouched =
          (preOwner != null
                  && preOwner.type() == Owner.Type.GROUP
                  && !sameOwner(preOwner, newOwner))
              || (newOwner != null && newOwner.type() == Owner.Type.GROUP);
      if (groupTouched) {
        ensureGroupAclEnabled();
      }

      String newPrincipal = newOwner != null ? formatOwnerPrincipal(newOwner) : null;

      if (preOwner != null && !sameOwner(preOwner, newOwner)) {
        String prePrincipal = formatOwnerPrincipal(preOwner);
        Set<AclBinding> candidateRemovals =
            KafkaPrivilegeMapper.ownerAclBindings(metadataObject, prePrincipal, properties);
        Map<MetadataObject, Optional<Owner>> ownerOverrides = new HashMap<>();
        ownerOverrides.put(metadataObject, Optional.ofNullable(newOwner));
        removeBindingsPreservingActiveGrants(
            preOwner.type(),
            preOwner.name(),
            candidateRemovals,
            Collections.emptySet(),
            Collections.emptySet(),
            ownerOverrides,
            null);
      }

      if (newPrincipal != null) {
        createAcls(KafkaPrivilegeMapper.ownerAclBindings(metadataObject, newPrincipal, properties));
      }
    }

    return Boolean.TRUE;
  }

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
    if (changes == null || changes.length == 0) {
      return Boolean.TRUE;
    }

    synchronized (aclSyncLock) {
      for (MetadataObjectChange change : changes) {
        if (change instanceof MetadataObjectChange.RemoveMetadataObject) {
          MetadataObject removed =
              ((MetadataObjectChange.RemoveMetadataObject) change).metadataObject();
          handleMetadataRemoved(removed);
        } else if (change instanceof MetadataObjectChange.RenameMetadataObject) {
          MetadataObjectChange.RenameMetadataObject rename =
              (MetadataObjectChange.RenameMetadataObject) change;
          handleMetadataRenamed(rename.metadataObject(), rename.newMetadataObject());
        } else if (change != null) {
          LOG.debug("Ignoring unsupported metadata object change: {}", change);
        }
      }
    }
    return Boolean.TRUE;
  }

  @Override
  public void close() throws IOException {
    adminClient.close(Duration.ofMillis(properties.getRequestTimeoutMs()));
  }

  private void applyRoleUpdateForPrincipal(
      Owner.Type principalType,
      String principalName,
      String kafkaPrincipal,
      Role preUpdateRole,
      RoleChange[] changes) {
    RoleChangeBindings delta = computeRoleChangeBindings(preUpdateRole, changes, kafkaPrincipal);
    Set<AclBinding> addedBindings =
        new LinkedHashSet<>(Sets.difference(delta.afterBindings, delta.beforeBindings));
    Set<AclBinding> candidateRemovals =
        new LinkedHashSet<>(Sets.difference(delta.beforeBindings, delta.afterBindings));

    createAcls(addedBindings);

    // PermissionManager commits the full post-update role to EntityStore before invoking
    // onRoleUpdated callbacks, so reconciling against persisted stateProvider roles accurately
    // reflects multi-object overridePrivilegesInRole updates.
    removeBindingsPreservingActiveGrants(
        principalType,
        principalName,
        candidateRemovals,
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptyMap(),
        null);
  }

  private RoleChangeBindings computeRoleChangeBindings(
      Role preUpdateRole, RoleChange[] changes, String kafkaPrincipal) {
    Map<String, SecurableObject> preUpdateByKey = new HashMap<>();
    if (preUpdateRole.securableObjects() != null) {
      for (SecurableObject existing : preUpdateRole.securableObjects()) {
        if (existing != null) {
          preUpdateByKey.put(securableObjectKey(existing), existing);
        }
      }
    }

    Map<String, Optional<SecurableObject>> initialTouched = new LinkedHashMap<>();
    Map<String, Optional<SecurableObject>> finalTouched = new LinkedHashMap<>();

    for (RoleChange change : changes) {
      if (change instanceof RoleChange.AddSecurableObject) {
        SecurableObject added = ((RoleChange.AddSecurableObject) change).getSecurableObject();
        if (added != null) {
          String key = securableObjectKey(added);
          initialTouched.computeIfAbsent(key, k -> Optional.empty());
          finalTouched.put(key, Optional.of(added));
        }
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        SecurableObject removed = ((RoleChange.RemoveSecurableObject) change).getSecurableObject();
        if (removed != null) {
          String key = securableObjectKey(removed);
          initialTouched.computeIfAbsent(
              key, k -> Optional.ofNullable(preUpdateByKey.getOrDefault(k, removed)));
          finalTouched.put(key, Optional.empty());
        }
      } else if (change instanceof RoleChange.UpdateSecurableObject) {
        RoleChange.UpdateSecurableObject update = (RoleChange.UpdateSecurableObject) change;
        SecurableObject oldObj = update.getSecurableObject();
        SecurableObject newObj = update.getNewSecurableObject();
        if (oldObj != null) {
          String oldKey = securableObjectKey(oldObj);
          initialTouched.computeIfAbsent(oldKey, k -> Optional.of(oldObj));
          finalTouched.put(oldKey, Optional.empty());
        }
        if (newObj != null) {
          String newKey = securableObjectKey(newObj);
          initialTouched.computeIfAbsent(
              newKey, k -> Optional.ofNullable(preUpdateByKey.get(newKey)));
          finalTouched.put(newKey, Optional.of(newObj));
        }
      }
    }

    Set<AclBinding> beforeBindings = new LinkedHashSet<>();
    for (Optional<SecurableObject> beforeObj : initialTouched.values()) {
      if (beforeObj.isPresent()) {
        beforeBindings.addAll(bindingsForSecurableObject(beforeObj.get(), kafkaPrincipal));
      }
    }

    Set<AclBinding> afterBindings = new LinkedHashSet<>();
    for (Optional<SecurableObject> afterObj : finalTouched.values()) {
      if (afterObj.isPresent()) {
        afterBindings.addAll(bindingsForSecurableObject(afterObj.get(), kafkaPrincipal));
      }
    }

    return new RoleChangeBindings(beforeBindings, afterBindings);
  }

  private void removeBindingsPreservingActiveGrants(
      Owner.Type principalType,
      String principalName,
      Set<AclBinding> candidateRemovals,
      Set<String> excludedRoleNames,
      Set<String> peerExcludedRoleNames,
      Map<MetadataObject, Optional<Owner>> ownerOverrides,
      @Nullable MetadataObject excludedObject) {
    if (candidateRemovals == null || candidateRemovals.isEmpty()) {
      return;
    }

    String kafkaPrincipal = formatPrincipal(principalType, principalName);
    Set<AclBinding> activeBindings =
        collectActiveAclBindingsForPrincipal(
            principalType,
            principalName,
            kafkaPrincipal,
            excludedRoleNames,
            ownerOverrides,
            excludedObject);
    if (sameUserAndGroupPrincipalType()) {
      Owner.Type peerType = principalType == Owner.Type.USER ? Owner.Type.GROUP : Owner.Type.USER;
      activeBindings.addAll(
          collectActiveAclBindingsForPrincipal(
              peerType,
              principalName,
              kafkaPrincipal,
              peerExcludedRoleNames,
              ownerOverrides,
              excludedObject));
    }

    Set<AclBinding> toDelete = Sets.difference(candidateRemovals, activeBindings);
    deleteExactAcls(toDelete);
  }

  private Set<AclBinding> collectActiveAclBindingsForPrincipal(
      Owner.Type principalType,
      String principalName,
      String kafkaPrincipal,
      Set<String> excludedRoleNames,
      Map<MetadataObject, Optional<Owner>> ownerOverrides,
      @Nullable MetadataObject excludedObject) {
    if (principalType == Owner.Type.GROUP && !properties.isGroupAclEnabled()) {
      return new LinkedHashSet<>();
    }
    Set<AclBinding> activeBindings =
        computeRoleBindingsForPrincipal(
            principalType, principalName, kafkaPrincipal, excludedRoleNames, excludedObject);
    activeBindings.addAll(
        computeOwnerBindingsForPrincipal(
            principalType, principalName, kafkaPrincipal, ownerOverrides, excludedObject));
    return activeBindings;
  }

  private Set<AclBinding> computeRoleBindingsForPrincipal(
      Owner.Type principalType,
      String principalName,
      String kafkaPrincipal,
      Set<String> excludedRoleNames,
      @Nullable MetadataObject excludedObject) {
    if (principalType == Owner.Type.GROUP && !properties.isGroupAclEnabled()) {
      return new LinkedHashSet<>();
    }
    Set<AclBinding> active = new LinkedHashSet<>();

    List<Role> storedRoles =
        principalType == Owner.Type.USER
            ? stateProvider.listRolesForUser(metalake, principalName)
            : stateProvider.listRolesForGroup(metalake, principalName);
    for (Role role : storedRoles) {
      if (role != null && !excludedRoleNames.contains(role.name())) {
        active.addAll(
            bindingsForRolesExcludingObject(
                Collections.singletonList(role), kafkaPrincipal, excludedObject));
      }
    }
    return active;
  }

  private Set<AclBinding> computeOwnerBindingsForPrincipal(
      Owner.Type principalType,
      String principalName,
      String kafkaPrincipal,
      Map<MetadataObject, Optional<Owner>> ownerOverrides,
      @Nullable MetadataObject excludedObject) {
    if (principalType == Owner.Type.GROUP && !properties.isGroupAclEnabled()) {
      return new LinkedHashSet<>();
    }
    Set<AclBinding> active = new LinkedHashSet<>();
    String resolvedCatalog = resolveCatalogName();
    List<MetadataObject> ownedObjects =
        stateProvider.listOwnedObjectsForPrincipal(
            metalake, resolvedCatalog, principalType, principalName);
    for (MetadataObject owned : ownedObjects) {
      if (owned == null || !appliesToThisCatalog(owned)) {
        continue;
      }
      if (excludedObject != null && sameMetadataObject(owned, excludedObject)) {
        continue;
      }
      Optional<Optional<Owner>> override = findOwnerOverride(ownerOverrides, owned);
      if (override.isPresent()) {
        Optional<Owner> effectiveOwner = override.get();
        if (effectiveOwner.isEmpty()
            || effectiveOwner.get().type() != principalType
            || !principalName.equals(effectiveOwner.get().name())) {
          continue;
        }
      }
      active.addAll(KafkaPrivilegeMapper.ownerAclBindings(owned, kafkaPrincipal, properties));
    }

    for (Map.Entry<MetadataObject, Optional<Owner>> entry : ownerOverrides.entrySet()) {
      MetadataObject overriddenObject = entry.getKey();
      Optional<Owner> effectiveOwner = entry.getValue();
      if (overriddenObject != null
          && !(excludedObject != null && sameMetadataObject(overriddenObject, excludedObject))
          && appliesToThisCatalog(overriddenObject)
          && effectiveOwner.isPresent()
          && effectiveOwner.get().type() == principalType
          && principalName.equals(effectiveOwner.get().name())) {
        active.addAll(
            KafkaPrivilegeMapper.ownerAclBindings(overriddenObject, kafkaPrincipal, properties));
      }
    }

    return active;
  }

  private Set<AclBinding> bindingsForRoles(List<Role> roles, String kafkaPrincipal) {
    return bindingsForRolesExcludingObject(roles, kafkaPrincipal, null);
  }

  private Set<AclBinding> bindingsForRolesExcludingObject(
      List<Role> roles, String kafkaPrincipal, @Nullable MetadataObject excludedObject) {
    Set<AclBinding> bindings = new LinkedHashSet<>();
    for (Role role : roles) {
      if (role == null || role.securableObjects() == null) {
        continue;
      }
      for (SecurableObject securableObject : role.securableObjects()) {
        if (excludedObject != null && sameMetadataObject(securableObject, excludedObject)) {
          continue;
        }
        bindings.addAll(bindingsForSecurableObject(securableObject, kafkaPrincipal));
      }
    }
    return bindings;
  }

  private Set<AclBinding> bindingsForSecurableObject(
      SecurableObject securableObject, String kafkaPrincipal) {
    if (securableObject == null || !appliesToThisCatalog(securableObject)) {
      return Collections.emptySet();
    }
    if (securableObject.privileges() == null) {
      return Collections.emptySet();
    }
    Set<AclBinding> bindings = new LinkedHashSet<>();
    for (Privilege privilege : securableObject.privileges()) {
      if (privilege != null) {
        bindings.addAll(
            KafkaPrivilegeMapper.toAclBindings(
                securableObject, privilege, kafkaPrincipal, properties));
      }
    }
    return Collections.unmodifiableSet(bindings);
  }

  private void handleMetadataRemoved(MetadataObject removed) {
    if (removed == null) {
      return;
    }
    // Dropping a Gravitino Kafka catalog removes metadata registration only, not Kafka broker
    // topics, so only explicit TOPIC removal deletes topic ACL bindings.
    if (removed.type() == MetadataObject.Type.TOPIC && appliesToThisCatalog(removed)) {
      AclBindingFilter filter =
          new AclBindingFilter(
              new ResourcePatternFilter(ResourceType.TOPIC, removed.name(), PatternType.LITERAL),
              AccessControlEntryFilter.ANY);
      Collection<AclBinding> deletedTopicAcls =
          deleteAclsByFilter(Collections.singletonList(filter));
      reconcileAuxiliaryAclsAfterTopicRemoval(removed, deletedTopicAcls);
    }
  }

  private void reconcileAuxiliaryAclsAfterTopicRemoval(
      MetadataObject removedTopic, Collection<AclBinding> deletedTopicAcls) {
    if (deletedTopicAcls == null || deletedTopicAcls.isEmpty()) {
      return;
    }

    Map<String, EnumSet<AclOperation>> deletedOpsByPrincipal = new HashMap<>();
    for (AclBinding deleted : deletedTopicAcls) {
      if (deleted == null || deleted.entry() == null) {
        continue;
      }
      if (deleted.entry().permissionType() != AclPermissionType.ALLOW) {
        continue;
      }
      AclOperation operation = deleted.entry().operation();
      if (operation != AclOperation.READ && operation != AclOperation.WRITE) {
        continue;
      }
      deletedOpsByPrincipal
          .computeIfAbsent(deleted.entry().principal(), k -> EnumSet.noneOf(AclOperation.class))
          .add(operation);
    }

    if (deletedOpsByPrincipal.isEmpty()) {
      return;
    }

    String userPrefix = properties.getUserPrincipalType() + ":";
    String groupPrefix = properties.getGroupPrincipalType() + ":";
    String host = properties.getAclHost();
    ResourcePattern groupPattern =
        new ResourcePattern(
            ResourceType.GROUP,
            properties.getConsumerGroupPattern(),
            properties.getConsumerGroupPatternType());
    ResourcePattern clusterPattern =
        new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);
    Map<MetadataObject, Optional<Owner>> removedOwnerOverride =
        Collections.singletonMap(removedTopic, Optional.empty());

    for (Map.Entry<String, EnumSet<AclOperation>> entry : deletedOpsByPrincipal.entrySet()) {
      String kafkaPrincipal = entry.getKey();
      Owner.Type principalType;
      String principalName;
      if (kafkaPrincipal.startsWith(userPrefix) && kafkaPrincipal.length() > userPrefix.length()) {
        principalType = Owner.Type.USER;
        principalName = kafkaPrincipal.substring(userPrefix.length());
      } else if (kafkaPrincipal.startsWith(groupPrefix)
          && kafkaPrincipal.length() > groupPrefix.length()) {
        principalType = Owner.Type.GROUP;
        principalName = kafkaPrincipal.substring(groupPrefix.length());
      } else {
        continue;
      }

      Set<AclBinding> candidateAuxRemovals = new LinkedHashSet<>();
      EnumSet<AclOperation> ops = entry.getValue();
      if (ops.contains(AclOperation.READ)) {
        candidateAuxRemovals.add(
            new AclBinding(
                groupPattern,
                new AccessControlEntry(
                    kafkaPrincipal, host, AclOperation.READ, AclPermissionType.ALLOW)));
      }
      if (ops.contains(AclOperation.WRITE) && properties.isIdempotentWriteAclEnabled()) {
        candidateAuxRemovals.add(
            new AclBinding(
                clusterPattern,
                new AccessControlEntry(
                    kafkaPrincipal, host, AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW)));
      }

      Set<AclBinding> activeBindings =
          collectActiveAclBindingsForPrincipal(
              principalType,
              principalName,
              kafkaPrincipal,
              Collections.emptySet(),
              removedOwnerOverride,
              removedTopic);
      if (userPrefix.equals(groupPrefix)) {
        Owner.Type peerType = principalType == Owner.Type.USER ? Owner.Type.GROUP : Owner.Type.USER;
        activeBindings.addAll(
            collectActiveAclBindingsForPrincipal(
                peerType,
                principalName,
                kafkaPrincipal,
                Collections.emptySet(),
                removedOwnerOverride,
                removedTopic));
      }
      deleteExactAcls(Sets.difference(candidateAuxRemovals, activeBindings));
    }
  }

  private void handleMetadataRenamed(MetadataObject oldObj, MetadataObject newObj) {
    if (oldObj == null || newObj == null) {
      return;
    }
    if (oldObj.type() == MetadataObject.Type.METALAKE && oldObj.name().equals(this.metalake)) {
      this.metalake = newObj.name();
      return;
    }
    if (oldObj.type() == MetadataObject.Type.CATALOG) {
      String currentCatalog = resolveCatalogName();
      if (currentCatalog == null || oldObj.name().equals(currentCatalog)) {
        this.catalogName = newObj.name();
      }
      return;
    }
    if (oldObj.type() == MetadataObject.Type.TOPIC
        && newObj.type() == MetadataObject.Type.TOPIC
        && appliesToThisCatalog(oldObj)) {
      AclBindingFilter filter =
          new AclBindingFilter(
              new ResourcePatternFilter(ResourceType.TOPIC, oldObj.name(), PatternType.LITERAL),
              AccessControlEntryFilter.ANY);
      Collection<AclBinding> existingBindings = describeAcls(filter);
      if (existingBindings.isEmpty()) {
        return;
      }

      Set<AclBinding> renamedBindings = new LinkedHashSet<>(existingBindings.size());
      for (AclBinding binding : existingBindings) {
        ResourcePattern newPattern =
            new ResourcePattern(ResourceType.TOPIC, newObj.name(), binding.pattern().patternType());
        renamedBindings.add(new AclBinding(newPattern, binding.entry()));
      }
      createAcls(renamedBindings);
      deleteExactAcls(existingBindings);
    }
  }

  private boolean appliesToThisCatalog(MetadataObject obj) {
    String resolvedCatalog = resolveCatalogName();
    switch (obj.type()) {
      case METALAKE:
        return this.metalake.equals(obj.fullName());
      case CATALOG:
        return resolvedCatalog == null || resolvedCatalog.equals(obj.fullName());
      case SCHEMA:
      case TOPIC:
        return resolvedCatalog == null || obj.fullName().startsWith(resolvedCatalog + ".");
      default:
        return false;
    }
  }

  private String resolveCatalogName() {
    if (this.catalogName != null) {
      return this.catalogName;
    }
    Long catalogId = properties.getCatalogId();
    if (catalogId != null) {
      Optional<String> resolved = stateProvider.resolveCatalogName(metalake, catalogId);
      if (resolved.isPresent()) {
        this.catalogName = resolved.get();
      }
    }
    return this.catalogName;
  }

  private void ensureGroupAclEnabled() {
    if (!properties.isGroupAclEnabled()) {
      throw new AuthorizationPluginException(
          "Kafka group ACL push-down is disabled by default because Apache Kafka's built-in"
              + " StandardAuthorizer does not expand group membership. Configure a group-aware"
              + " Kafka Authorizer or KafkaPrincipalBuilder on the broker and set %s=true on the"
              + " catalog to enable group ACLs.",
          KafkaAuthorizationProperties.GROUP_ACL_ENABLED);
    }
  }

  private boolean sameUserAndGroupPrincipalType() {
    return properties.getUserPrincipalType().equals(properties.getGroupPrincipalType());
  }

  private static boolean isWildcardIdentityName(@Nullable String name) {
    return name != null && "*".equals(name.trim());
  }

  private String formatOwnerPrincipal(Owner owner) {
    return formatPrincipal(owner.type(), owner.name());
  }

  private String formatPrincipal(Owner.Type type, String name) {
    if (type == Owner.Type.USER) {
      return properties.formatUserPrincipal(name);
    }
    if (type == Owner.Type.GROUP) {
      return properties.formatGroupPrincipal(name);
    }
    throw new IllegalArgumentException("Unsupported owner type: " + type);
  }

  private void createAcls(Collection<AclBinding> bindings) {
    if (bindings == null || bindings.isEmpty()) {
      return;
    }
    List<AclBinding> uniqueBindings = new ArrayList<>(new LinkedHashSet<>(bindings));
    try {
      adminClient.createAcls(uniqueBindings).all().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AuthorizationPluginException(e, "Interrupted while creating Kafka ACLs");
    } catch (ExecutionException e) {
      throw new AuthorizationPluginException(
          e.getCause() != null ? e.getCause() : e,
          "Failed to create %d Kafka ACL binding(s)",
          uniqueBindings.size());
    }
  }

  private void deleteExactAcls(Collection<AclBinding> bindings) {
    if (bindings == null || bindings.isEmpty()) {
      return;
    }
    Set<AclBindingFilter> filters = new LinkedHashSet<>(bindings.size());
    for (AclBinding binding : bindings) {
      filters.add(binding.toFilter());
    }
    deleteAclsByFilter(filters);
  }

  private Collection<AclBinding> deleteAclsByFilter(Collection<AclBindingFilter> filters) {
    if (filters == null || filters.isEmpty()) {
      return Collections.emptyList();
    }
    List<AclBindingFilter> uniqueFilters = new ArrayList<>(new LinkedHashSet<>(filters));
    try {
      Collection<AclBinding> deleted = adminClient.deleteAcls(uniqueFilters).all().get();
      return deleted == null
          ? Collections.emptyList()
          : Collections.unmodifiableCollection(deleted);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AuthorizationPluginException(e, "Interrupted while deleting Kafka ACLs");
    } catch (ExecutionException e) {
      throw new AuthorizationPluginException(
          e.getCause() != null ? e.getCause() : e,
          "Failed to delete Kafka ACLs matching %d filter(s)",
          uniqueFilters.size());
    }
  }

  private Collection<AclBinding> describeAcls(AclBindingFilter filter) {
    try {
      Collection<AclBinding> result = adminClient.describeAcls(filter).values().get();
      return result == null ? Collections.emptyList() : Collections.unmodifiableCollection(result);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AuthorizationPluginException(e, "Interrupted while describing Kafka ACLs");
    } catch (ExecutionException e) {
      throw new AuthorizationPluginException(
          e.getCause() != null ? e.getCause() : e, "Failed to describe Kafka ACLs");
    }
  }

  private static Set<String> extractRoleNames(List<Role> roles) {
    Set<String> names = new HashSet<>(roles.size());
    for (Role role : roles) {
      if (role != null && role.name() != null) {
        names.add(role.name());
      }
    }
    return names;
  }

  private static Optional<Optional<Owner>> findOwnerOverride(
      Map<MetadataObject, Optional<Owner>> overrides, MetadataObject target) {
    for (Map.Entry<MetadataObject, Optional<Owner>> entry : overrides.entrySet()) {
      if (sameMetadataObject(entry.getKey(), target)) {
        return Optional.of(entry.getValue());
      }
    }
    return Optional.empty();
  }

  private static String securableObjectKey(SecurableObject obj) {
    return obj.type() + ":" + obj.fullName();
  }

  private static boolean sameMetadataObject(MetadataObject left, MetadataObject right) {
    if (left == right) {
      return true;
    }
    if (left == null || right == null) {
      return false;
    }
    return left.type() == right.type() && Objects.equals(left.fullName(), right.fullName());
  }

  private static boolean sameOwner(Owner left, Owner right) {
    if (left == right) {
      return true;
    }
    if (left == null || right == null) {
      return false;
    }
    return left.type() == right.type() && Objects.equals(left.name(), right.name());
  }

  private static final class RoleChangeBindings {

    private final Set<AclBinding> beforeBindings;
    private final Set<AclBinding> afterBindings;

    private RoleChangeBindings(Set<AclBinding> beforeBindings, Set<AclBinding> afterBindings) {
      this.beforeBindings = beforeBindings;
      this.afterBindings = afterBindings;
    }
  }
}
