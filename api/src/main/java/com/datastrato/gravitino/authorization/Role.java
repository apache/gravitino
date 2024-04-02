/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.List;
import java.util.Map;

/** The interface of a role. The role is the entity which has kinds of privileges. */
@Evolving
public interface Role extends Auditable {

  /**
   * The name of the role.
   *
   * @return The name of the role.
   */
  String name();

  /**
   * The properties of the group. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the role.
   */
  Map<String, String> properties();

  /**
   * The privileges of the role.
   *
   * @return The privileges of the role.
   */
  List<Privilege> privileges();

  /**
   * The privilege entity identifier of the role.
   *
   * @return The privilege entity identifier of the role.
   */
  NameIdentifier privilegeEntityIdentifier();

  /**
   * The privilege entity type of the role.
   *
   * @return The privilege entity type of the role.
   */
  String privilegeEntityType();
}
