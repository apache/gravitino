/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test.util;

import com.datastrato.graviton.integration.util.EnvironmentVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EnvironmentVariableTest {
  @Test
  public void testInjectEnv() throws Exception {
    String gravitonHome = "/tmp";
    EnvironmentVariable.injectEnv("GRAVITON_HOME", gravitonHome);
    Assertions.assertEquals(gravitonHome, System.getenv("GRAVITON_HOME"));
  }
}
