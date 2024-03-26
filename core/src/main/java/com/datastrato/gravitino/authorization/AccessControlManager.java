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
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AccessControlManager is used for manage users, roles, grant information, this class is an
 * entrance class for tenant management.
 */
public class AccessControlManager {

  private static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in th metalake %s";

  private static final Logger LOG = LoggerFactory.getLogger(AccessControlManager.class);

  private final EntityStore store;
  private final IdGenerator idGenerator;

  /**
   * Constructs a AccessControlManager instance.
   *
   * @param store The EntityStore to use for managing access control.
   * @param idGenerator The IdGenerator to use for generating identifiers.
   */
  public AccessControlManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  /**
   * Adds a new User.
   *
   * @param metalake The Metalake of the User.
   * @param name The name of the User.
   * @return The added User instance.
   * @throws UserAlreadyExistsException If a User with the same identifier already exists.
   * @throws RuntimeException If adding the User encounters storage issues.
   */
  public User addUser(String metalake, String name) throws UserAlreadyExistsException {
    UserEntity userEntity =
        UserEntity.builder()
            .withId(idGenerator.nextId())
            .withName(name)
            .withMetalake(metalake)
            .withNamespace(
                Namespace.of(
                    metalake,
                    AuthorizationConstants.SYSTEM_CATALOG_RESERVED_NAME,
                    AuthorizationConstants.USER_SCHEMA_NAME))
            .withGroups(Lists.newArrayList())
            .withRoles(Lists.newArrayList())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(userEntity, false /* overwritten */);
      return userEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake {} already exists", name, metalake, e);
      throw new UserAlreadyExistsException(
          "User %s in the metalake %s already exists", name, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding user {} failed in the metalake {} due to storage issues", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Removes a User.
   *
   * @param metalake The Metalake of the User.
   * @param user THe name of the User.
   * @return `true` if the User was successfully removed, `false` otherwise.
   * @throws RuntimeException If removing the User encounters storage issues.
   */
  public boolean removeUser(String metalake, String user) {

    try {
      return store.delete(
          NameIdentifier.of(
              metalake,
              AuthorizationConstants.SYSTEM_CATALOG_RESERVED_NAME,
              AuthorizationConstants.USER_SCHEMA_NAME),
          Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} in the metalake {} failed due to storage issues", user, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Gets a User.
   *
   * @param metalake The Metalake of the User.
   * @param user The name of the User.
   * @return The getting User instance.
   * @throws NoSuchUserException If the User with the given identifier does not exist.
   * @throws RuntimeException If getting the User encounters storage issues.
   */
  public User getUser(String metalake, String user) throws NoSuchUserException {
    try {
      return store.get(
          NameIdentifier.of(
              metalake,
              AuthorizationConstants.SYSTEM_CATALOG_RESERVED_NAME,
              AuthorizationConstants.USER_SCHEMA_NAME),
          Entity.EntityType.USER,
          UserEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("user {} does not exist in the metalake {}", user, metalake, e);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting user {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }
}
