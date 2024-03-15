/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import java.util.List;
import java.util.Map;

/** The interface of a Group. The Group is the entity which contains users. */
public interface Group extends Auditable {

  /**
   * The name of the group.
   *
   * @return The name of the group.
   */
  String name();

  /**
   * The properties of the group. Note, this method will return null if the properties are not set.
   *
   * @return The properties of the group.
   */
  Map<String, String> properties();

  /**
   * The users of the group.
   *
   * @return The users of the group.
   */
  List<String> users();
}
