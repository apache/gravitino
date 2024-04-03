/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAccessControlNotAllowedFilter {

  @Test
  public void testAccessControlPath() {
    AccessControlNotAllowedFilter filter = new AccessControlNotAllowedFilter();
    Assertions.assertTrue(filter.isAccessControlPath("/api/admins"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/admins/"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake/"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/users"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/users/"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/users/user"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/users/userRandom/"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/groups"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/groups/"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/groups/group1"));
    Assertions.assertTrue(
        filter.isAccessControlPath("/api/metalakes/metalake/groups/groupRandom/"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake/catalogs"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake/catalogs/"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake/catalogs/catalog1"));
  }
}
