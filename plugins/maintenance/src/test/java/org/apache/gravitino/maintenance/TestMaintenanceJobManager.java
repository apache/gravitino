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
package org.apache.gravitino.maintenance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MaintenanceJobManager}. */
public class TestMaintenanceJobManager {

  private static final String METALAKE = "ml";

  @Test
  void testRequireMetalakeOwnerUserReturnsUserName() {
    OwnerDispatcher owners = mock(OwnerDispatcher.class);
    Owner owner = mock(Owner.class);
    when(owner.type()).thenReturn(Owner.Type.USER);
    when(owner.name()).thenReturn("admin");
    when(owners.getOwner(
            eq(METALAKE), eq(MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE))))
        .thenReturn(Optional.of(owner));

    Assertions.assertEquals(
        "admin", MaintenanceJobManager.requireMetalakeOwnerUser(owners, METALAKE));
  }

  @Test
  void testRequireMetalakeOwnerUserRejectsMissingOwner() {
    OwnerDispatcher owners = mock(OwnerDispatcher.class);
    when(owners.getOwner(eq(METALAKE), any(MetadataObject.class))).thenReturn(Optional.empty());

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> MaintenanceJobManager.requireMetalakeOwnerUser(owners, METALAKE));
  }

  @Test
  void testRequireMetalakeOwnerUserRejectsGroupOwner() {
    OwnerDispatcher owners = mock(OwnerDispatcher.class);
    Owner owner = mock(Owner.class);
    when(owner.type()).thenReturn(Owner.Type.GROUP);
    when(owner.name()).thenReturn("admins");
    when(owners.getOwner(eq(METALAKE), any(MetadataObject.class))).thenReturn(Optional.of(owner));

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> MaintenanceJobManager.requireMetalakeOwnerUser(owners, METALAKE));
  }
}
