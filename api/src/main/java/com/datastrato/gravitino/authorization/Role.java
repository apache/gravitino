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
 * multiple privileges of one resource. Gravitino chooses to binds one resource to one role to avoid
 * granting too many privileges to one role.
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
   * The privileges of the role. All privileges belong to one resource. For example: If the resource
   * is a table, the privileges could be `READ TABLE`, `WRITE TABLE`, etc. If a schema has the
   * privilege of `LOAD TABLE`. It means the role can all tables of the schema.
   *
   * @return The privileges of the role.
   */
  List<Privilege> privileges();

  /**
   * One role contains one resource. For example: If the resource is a table, the
   * identifier may be `catalog1.schema1.table1`.
   *
   * @return The resource of the role.
   */
  Resource resource();
}
