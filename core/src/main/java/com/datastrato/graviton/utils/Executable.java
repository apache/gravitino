/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.utils;

@FunctionalInterface
public interface Executable<R, E extends Exception> {

  R execute() throws E;
}
