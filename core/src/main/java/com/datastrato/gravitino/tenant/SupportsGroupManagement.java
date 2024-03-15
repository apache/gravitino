/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.Group;
import java.util.List;
import java.util.Map;

public interface SupportsGroupManagement {

  /**
   * Creates a new Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @param properties Additional properties for the Metalake.
   * @return The created Group instance.
   * @throws GroupAlreadyExistsException If a Group with the same identifier already exists.
   * @throws RuntimeException If creating the Group encounters storage issues.
   */
  Group createGroup(
      String metalake, String group, List<String> users, Map<String, String> properties);

  /**
   * Deletes a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return `true` if the Group was successfully deleted, `false` otherwise.
   * @throws RuntimeException If deleting the Group encounters storage issues.
   */
  boolean dropGroup(String metalake, String group);

  /**
   * Loads a Group.
   *
   * @param metalake The Metalake of the Group.
   * @param group THe name of the Group.
   * @return The loaded Group instance.
   * @throws NoSuchGroupException If the Group with the given identifier does not exist.
   * @throws RuntimeException If loading the Group encounters storage issues.
   */
  Group loadGroup(String metalake, String group);
}
