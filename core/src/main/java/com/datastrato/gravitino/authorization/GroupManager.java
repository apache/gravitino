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
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GroupManager is used for add, remove and get groups from one metalake. GroupManager doesn't
 * manage group, just sets up the relationship between the metalake and the group. Metalake is like
 * a concept of the organization. `AddGroup` means that a group enter an organization.
 */
public class GroupManager {
  private static final Logger LOG = LoggerFactory.getLogger(GroupManager.class);

  private static final String GROUP_DOES_NOT_EXIST_MSG =
      "Group %s does not exist in th metalake %s";

  private final EntityStore store;
  private final IdGenerator idGenerator;

  public GroupManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  /**
   * Adds a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group The name of the Group.
   * @return The Added Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same identifier already exists.
   * @throws RuntimeException If creating the Group encounters storage issues.
   */
  public Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    GroupEntity metalakeGroup =
        GroupEntity.builder()
            .withId(idGenerator.nextId())
            .withName(group)
            .withNamespace(
                Namespace.of(
                    metalake,
                    CatalogEntity.SYSTEM_CATALOG_RESERVED_NAME,
                    GroupEntity.GROUP_SCHEMA_NAME))
            .withRoles(Collections.emptyList())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(metalakeGroup, false /* overwritten */);
      return metalakeGroup;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Group {} in the metalake {} already exists", group, metalake, e);
      throw new GroupAlreadyExistsException(
          "Group %s in the metalake %s already exists", group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding group {} failed in the metalake {} due to storage issues", group, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Removes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return `true` if the Group was successfully deleted, `false` otherwise.
   * @throws RuntimeException If deleting the Group encounters storage issues.
   */
  public boolean removeGroup(String metalake, String group) {
    try {
      return store.delete(ofGroup(metalake, group), Entity.EntityType.GROUP);
    } catch (IOException ioe) {
      LOG.error(
          "Removing group {} in the metalake {} failed due to storage issues",
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Gets a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return The getting Group instance.
   * @throws NoSuchGroupException If the Group with the given identifier does not exist.
   * @throws RuntimeException If loading the Group encounters storage issues.
   */
  public Group getGroup(String metalake, String group) {
    try {
      return store.get(ofGroup(metalake, group), Entity.EntityType.GROUP, GroupEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Group {} does not exist in the metalake {}", group, metalake, e);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting group {} failed due to storage issues", group, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private NameIdentifier ofGroup(String metalake, String group) {
    return NameIdentifier.of(
        metalake, CatalogEntity.SYSTEM_CATALOG_RESERVED_NAME, GroupEntity.GROUP_SCHEMA_NAME, group);
  }
}
