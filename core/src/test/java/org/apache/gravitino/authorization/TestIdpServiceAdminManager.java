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

package org.apache.gravitino.authorization;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIdpServiceAdminManager {

  private IdpUserManager userManager;
  private IdpUserMetaService userMetaService;
  private IdpServiceAdminManager serviceAdminManager;

  @BeforeEach
  public void setUp() {
    userManager = Mockito.mock(IdpUserManager.class);
    userMetaService = Mockito.mock(IdpUserMetaService.class);
    serviceAdminManager = new IdpServiceAdminManager(userManager, userMetaService);
  }

  @Test
  public void testServiceAdminExists() {
    when(userMetaService.findUser("admin1")).thenReturn(Optional.of(Mockito.mock(IdpUserPO.class)));
    when(userMetaService.findUser("admin2")).thenReturn(Optional.empty());

    assertTrue(serviceAdminManager.serviceAdminExists("admin1"));
    assertFalse(serviceAdminManager.serviceAdminExists("admin2"));
  }

  @Test
  public void testInitializeServiceAdmin() {
    serviceAdminManager.initializeServiceAdmin("admin1", "Passw0rd-For-Admin1");

    verify(userManager).createUser("admin1", "Passw0rd-For-Admin1");
  }
}
