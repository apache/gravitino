/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.utils;

import java.util.function.Predicate;

/**
 * Wraper for {@link Predicate} that throws exception. To be more specific, it wraps a predicate
 * that throws {@link RuntimeException} or it's subclass.
 */
@FunctionalInterface
public interface NonThrowablePredicate<IN, E extends Exception> {
  boolean test(IN in) throws E;

  static <T, E extends Exception> Predicate<T> wrap(NonThrowablePredicate<T, E> predicate) {
    return v -> {
      try {
        return predicate.test(v);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }
}
