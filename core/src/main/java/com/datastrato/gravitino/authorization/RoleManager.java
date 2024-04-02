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

  public RoleManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  /**
   * Creates a new Role.
   *
   * @param metalake The Metalake of the Role.
   * @param role The name of the Role.
   * @param properties The properties of the Role.
   * @param privilegeEntityIdentifier The privilege entity identifier of the Role.
   * @param privilegeEntityType The privilege entity type of the Role.
   * @param privileges The privileges of the Role.
   * @return The created Role instance.
   * @throws RoleAlreadyExistsException If a Role with the same identifier already exists.
   * @throws RuntimeException If creating the Role encounters storage issues.
   */
  public Role createRole(
      String metalake,
      String role,
      Map<String, String> properties,
      NameIdentifier privilegeEntityIdentifier,
      Entity.EntityType privilegeEntityType,
      List<Privilege> privileges)
      throws RoleAlreadyExistsException {
    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(idGenerator.nextId())
            .withName(role)
            .withProperties(properties)
            .withPrivilegeEntityIdentifier(privilegeEntityIdentifier)
            .withPrivilegeEntityType(privilegeEntityType)
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
      return store.get(ofRole(metalake, role), Entity.EntityType.ROLE, RoleEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Role {} does not exist in the metalake {}", role, metalake, e);
      throw new NoSuchRoleException(ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    } catch (IOException ioe) {
      LOG.error("Loading role {} failed due to storage issues", role, ioe);
      throw new RuntimeException(ioe);
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
      return store.delete(ofRole(metalake, role), Entity.EntityType.ROLE);
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
