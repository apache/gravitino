/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.util;

import java.io.IOException;

@FunctionalInterface
public interface Executable<R> {

  R execute() throws IOException;
}
