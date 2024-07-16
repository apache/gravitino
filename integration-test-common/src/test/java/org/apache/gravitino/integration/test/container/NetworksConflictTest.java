/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

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
