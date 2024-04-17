/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Evolving;

/**
 * The interface of a privilege. The privilege represents the ability to execute kinds of operations
 * for kinds of entities
 */
@Evolving
public interface Privilege {

  /** @return The generic name of the privilege. */
  Name name();

  /** @return A readable string representation for the privilege. */
  String simpleString();

  /** The name of this privilege. */
  enum Name {
    /** The privilege of load a catalog. */
    LOAD_CATALOG
  }
}
