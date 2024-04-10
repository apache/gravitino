/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.List;

/** The interface of a Group. The Group is the entity which contains users. */
@Evolving
public interface Group extends Auditable {

  /**
   * The name of the group.
   *
   * @return The name of the group.
   */
  String name();

  /**
   * The roles of the group.
   *
   * @return The roles of the group.
   */
  List<String> roles();
}
