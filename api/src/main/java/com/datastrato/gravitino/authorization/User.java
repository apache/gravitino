/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Auditable;
import java.util.List;
import java.util.Map;

/** The interface of a user. The user is the entity which executes every operation. */
public interface User extends Auditable {

  /**
   * The name of the user.
   *
   * @return The name of the user.
   */
  String name();

  /**
   * The properties of the user. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the user.
   */
  Map<String, String> properties();

  /**
   * The first name of the user.
   *
   * @return The first name of the user.
   */
  String firstName();

  /**
   * The last name of the user.
   *
   * @return The last name of the user.
   */
  String lastName();

  /**
   * The display name of the user.
   *
   * @return The display name of the user.
   */
  String displayName();

  /**
   * The email address of the user.
   *
   * @return The email address of the user.
   */
  String emailAddress();

  /**
   * The comment of the user.
   *
   * @return The comment of the user.
   */
  String comment();

  /**
   * The default role of the user.
   *
   * @return The default role of the user.
   */
  String defaultRole();

  /**
   * The status of the user is whether active or not.
   *
   * @return The status of the user is whether active or not
   */
  boolean active();

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
