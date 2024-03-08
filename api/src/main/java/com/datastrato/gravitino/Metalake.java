/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Map;

/**
 * The interface of a metalake. The metalake is the top level entity in the gravitino system,
 * containing a set of catalogs.
 */
@Evolving
public interface Metalake extends Auditable {

  /**
   * The name of the metalake.
   *
   * @return The name of the metalake.
   */
  String name();

  /**
   * The comment of the metalake. Note. this method will return null if the comment is not set for
   * this metalake.
   *
   * @return The comment of the metalake.
   */
  String comment();

  /**
   * The properties of the metalake. Note, this method will return null if the properties are not
   * set.
   *
   * @return The properties of the metalake.
   */
  Map<String, String> properties();
}
