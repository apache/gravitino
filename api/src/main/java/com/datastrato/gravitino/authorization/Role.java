/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.List;
import java.util.Map;

/**
 * The interface of a role. The role is the entity which has kinds of privileges. One role can have
 * multiple privileges of one securable object. Gravitino chooses to bind one securable object to
 * one role to avoid granting too many privileges to one role.
 */
@Evolving
public interface Role extends Auditable {

  /**
   * The name of the role.
   *
   * @return The name of the role.
   */
  String name();

  /**
   * The properties of the role. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the role.
   */
  Map<String, String> properties();

  /**
   * The securable object represents a special kind of entity with a unique identifier. All
   * securable objects are organized by tree structure. For example: If the securable object is a
   * table, the identifier may be `catalog1.schema1.table1`.
   *
   * @return The securable objects of the role.
   */
  List<SecurableObject> securableObjects();
}
