/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.utils;

import java.util.function.Function;

/**
 * Wraper for function that throws exception. To be more specific, it wraps a function that throws
 * {@link RuntimeException} or it's subclass.
 */
@FunctionalInterface
public interface NonThrowableFunction<T, R, E extends Exception> {

  R apply(T t) throws E;

  static <T, R, E extends Exception> Function<T, R> wraper(NonThrowableFunction<T, R, E> function) {
    return v -> {
      try {
        return function.apply(v);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }
}
