/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NetworksConflictTest {
  @Test
  public void networksConflictTest1() throws Exception {
    final String subnet1 = "10.20.30.0/28"; // allocate IP ranger 10.20.30.1 ~ 10.20.30.14
    final String subnet2 = "10.20.30.0/26"; // allocate IP ranger is 10.20.30.1 ~ 10.20.30.62
    Assertions.assertTrue(ContainerSuite.ipRangesOverlap(subnet1, subnet2));
  }

  @Test
  public void networksConflictTest2() throws Exception {
    final String subnet1 = "10.20.30.0/28"; // allocate IP ranger is 10.20.30.1 ~ 10.20.30.14
    final String subnet2 = "10.20.31.0/28"; // allocate IP ranger is 10.20.31.1 ~ 10.20.31.14
    Assertions.assertFalse(ContainerSuite.ipRangesOverlap(subnet1, subnet2));
  }
}
