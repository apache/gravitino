/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import java.io.IOException;

/**
 * Predicate for a key-value pair. It's a little like {@link java.util.function.Predicate}, but it
 * throws {@link IOException} and has two parameters k and v, which are key and value.
 */
@FunctionalInterface
public interface KvPredicate {
  boolean test(byte[] key, byte[] value) throws IOException;
}
