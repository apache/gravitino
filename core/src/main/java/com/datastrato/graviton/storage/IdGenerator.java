/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

/** Generate a unique id that maps to a name. */
public interface IdGenerator {

  /** Returns a unique identifier. */
  long nextId();
}
