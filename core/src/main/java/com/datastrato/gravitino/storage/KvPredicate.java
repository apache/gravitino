/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import java.io.IOException;

@FunctionalInterface
public interface KvPredicate {
  boolean test(byte[] key, byte[] value) throws IOException;
}
