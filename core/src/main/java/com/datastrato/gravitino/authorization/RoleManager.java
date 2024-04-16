/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.github.benmanes.caffeine.cache.Cache;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RoleManager is responsible for managing the roles. Role contains the privileges of one privilege
 * entity. If one Role is created and the privilege entity is an external system, the role will be
 * created in the underlying entity, too.
 */
class RoleManager {

  private static final Logger LOG = LoggerFactory.getLogger(RoleManager.class);
  private static final String ROLE_DOES_NOT_EXIST_MSG = "Role %s does not exist in th metalake %s";
  private final EntityStore store;
  private final IdGenerator idGenerator;
  private final Cache<NameIdentifier, RoleEntity> cache;

  public RoleManager(
      EntityStore store, IdGenerator idGenerator, Cache<NameIdentifier, RoleEntity> cache) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.cache = cache;
  }

  /**
   * Creates a new Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @param properties The properties of the Role.
   * @param securableObject The securable object of the Role.
   * @param privileges The privileges of the Role.
   * @return The created Role instance.
   * @throws RoleAlreadyExistsException If a Role with the same identifier already exists.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      SecurableObject securableObject,
      List<Privilege> privileges)
      throws RoleAlreadyExistsException {
    AuthorizationUtils.checkMetalakeExists(metalake);
    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(idGenerator.nextId())
            .withName(role)
            .withProperties(properties)
            .withSecurableObject(securableObject)
            .withPrivileges(privileges)
            .withNamespace(
                Namespace.of(
                    metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(roleEntity, false /* overwritten */);
      cache.put(roleEntity.nameIdentifier(), roleEntity);
      return roleEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Role {} in the metalake {} already exists", role, metalake, e);
      throw new RoleAlreadyExistsException(
          "Role %s in the metalake %s already exists", role, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Creating role {} failed in the metalake {} due to storage issues", role, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Loads a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return The loading Role instance.
   * @throws NoSuchRoleException If the Role with the given identifier does not exist.
   * @throws RuntimeException If loading the Role encounters storage issues.
   */
  public Role loadRole(String metalake, String role) throws NoSuchRoleException {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      return AuthorizationUtils.getRoleEntity(ofRole(metalake, role));
    } catch (NoSuchEntityException e) {
      LOG.warn("Role {} does not exist in the metalake {}", role, metalake, e);
      throw new NoSuchRoleException(ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    }
  }

  /**
   * Drops a Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @return `true` if the Role was successfully dropped, `false` otherwise.
   * @throws RuntimeException If dropping the User encounters storage issues.
   */
  public boolean dropRole(String metalake, String role) {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      NameIdentifier ident = ofRole(metalake, role);
      cache.invalidate(ident);

      return store.delete(ident, Entity.EntityType.ROLE);
    } catch (IOException ioe) {
      LOG.error(
          "Deleting role {} in the metalake {} failed due to storage issues", role, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private NameIdentifier ofRole(String metalake, String role) {
    return NameIdentifier.of(
        metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME, role);
  }
}
