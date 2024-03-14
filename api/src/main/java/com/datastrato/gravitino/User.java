/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import java.util.Map;

public interface User extends Auditable {

  /**
   * The name of the user.
   *
   * @return The name of the user.
   */
  String name();

  /**
   * The properties of the user. Note, this method will return null if the properties are not
   * set.
   *
   * @return The properties of the user.
   */
  Map<String, String> properties();
}
