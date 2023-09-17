/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.test.core;

import com.datastrato.graviton.integration.test.util.ITUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ITUtilsTest {
  @Test
  public void testInjectEnv() throws Exception {
    String envInjectVar = "tmp";
    ITUtils.injectEnvironment("ENV_INJECT_NAME", envInjectVar);
    Assertions.assertEquals(envInjectVar, System.getenv("ENV_INJECT_NAME"));
  }
}
