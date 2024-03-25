/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Auditable;
import java.util.List;

/** The interface of a user. The user is the entity which executes every operation. */
public interface User extends Auditable {

  /**
   * The name of the user.
   *
   * @return The name of the user.
   */
  String name();

  /**
   * The roles of the user.
   *
   * @return The roles of the user.
   */
  List<String> roles();

  /**
   * The groups of the user.
   *
   * @return The groups of the user.
   */
  List<String> groups();
}
