/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.utils;

@FunctionalInterface
public interface ThrowableFunction<T, R> {

  R apply(T t) throws Exception;
}
