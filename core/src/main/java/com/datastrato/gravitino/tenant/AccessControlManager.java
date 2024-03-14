/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.MetalakeUser;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* AccessControlManager is used for manage users, roles, grant information, this class is
 * an important class for tenant management. */
public class AccessControlManager implements SupportsUserManagement {

  private static final String USER_DOES_NOT_EXIST_MSG = "User %s does not exist in th metalake %s";

  private static final Logger LOG = LoggerFactory.getLogger(AccessControlManager.class);

  private final EntityStore store;
  private final IdGenerator idGenerator;

  public AccessControlManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  @Override
  public com.datastrato.gravitino.User createUser(
      String metalake, String userName, Map<String, String> properties) {
    MetalakeUser metalakeUser =
        MetalakeUser.builder()
            .withId(idGenerator.nextId())
            .withName(userName)
            .withMetalake(metalake)
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(metalakeUser, false /* overwritten */);
      return metalakeUser;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake {} already exists", userName, metalake, e);
      throw new UserAlreadyExistsException(
          "User %s in the metalake %s already exists", userName, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Creating user {} failed in the metalake {} due to storage issues",
          userName,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public boolean dropUser(String metalake, String userName) {

    try {
      return store.delete(NameIdentifier.of(metalake, userName), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Deleting user {} in the metalake {} failed due to storage issues",
          userName,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public com.datastrato.gravitino.User loadUser(String metalake, String userName) {
    try {
      return store.get(
          NameIdentifier.of(metalake, userName), Entity.EntityType.USER, MetalakeUser.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("user {} does not exist in the metalake {}", userName, metalake, e);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, userName, metalake);
    } catch (IOException ioe) {
      LOG.error("Loading user {} failed due to storage issues", userName, ioe);
      throw new RuntimeException(ioe);
    }
  }
}
