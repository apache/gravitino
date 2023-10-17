/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestApiVersion {

  @Test
  public void testLatestVersion() {
    ApiVersion latest = ApiVersion.latestVersion();
    assertEquals(ApiVersion.V_1, latest);
  }

  @Test
  public void testIsSupportedVersion() {
    assertTrue(ApiVersion.isSupportedVersion(1));
    assertFalse(ApiVersion.isSupportedVersion(2));
  }
}
