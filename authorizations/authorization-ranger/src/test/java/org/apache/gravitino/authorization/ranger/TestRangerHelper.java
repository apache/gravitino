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
package org.apache.gravitino.authorization.ranger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.ranger.RangerClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRangerHelper {

  @Test
  public void testGenerateCatalogOwnerRoleName() {
    Assertions.assertEquals(
        "GRAVITINO_CATALOG_OWNER_ROLE_1001", RangerHelper.generateCatalogOwnerRoleName("1001"));
    Assertions.assertNotEquals(
        RangerHelper.generateCatalogOwnerRoleName("1001"),
        RangerHelper.generateCatalogOwnerRoleName("1002"));
  }

  @Test
  public void testCreateCatalogSpecificOwnerRole() {
    RangerHelper rangerHelper =
        new RangerHelper(
            Mockito.mock(RangerClient.class),
            "admin",
            "service",
            ImmutableSet.of(),
            ImmutableList.of());

    Assertions.assertDoesNotThrow(
        () ->
            rangerHelper.createRangerRoleIfNotExists(
                RangerHelper.generateCatalogOwnerRoleName("1001"), true));
  }
}
