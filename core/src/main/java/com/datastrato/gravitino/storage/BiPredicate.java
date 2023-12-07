/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import java.io.IOException;

/**
 * Predicate for two parameters. It's a little like {@link java.util.function.Predicate}, but it
 * throws {@link IOException} and has two parameters v1 and v2.
 */
@FunctionalInterface
public interface BiPredicate<T1, T2> {
  boolean test(T1 v1, T2 v2) throws IOException;
}
