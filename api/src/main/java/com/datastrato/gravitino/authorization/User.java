/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.annotation.Evolving;

import java.util.List;

/** The interface of a user. The user is the entity which executes every operation. */
@Evolving
public interface User extends Auditable {

  /**
   * The name of the user. It's the identifier of User. It must be unique. Usually the name comes
   * from a external user management system like LDAP, IAM and so on.
   *
   * @return The name of the user.
   */
  String name();

  /**
   * The roles of the user. A user can have multiple roles. Every role binds several privileges.
   *
   * @return The roles of the user.
   */
  List<String> roles();
}
