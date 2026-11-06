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

package org.apache.gravitino.cli;

import java.util.Locale;
import org.apache.gravitino.authorization.Privilege;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCatalogCredentialPrivileges {
  @Test
  void testCredentialGrantsAreAcceptedByCli() {
    for (Privilege.Name name :
        new Privilege.Name[] {
          Privilege.Name.USE_SECRETS, Privilege.Name.INCLUDE_CREDENTIAL_SECRETS
        }) {
      String option = name.name().toLowerCase(Locale.ROOT);
      Assertions.assertTrue(Privileges.isValid(option));
      Assertions.assertEquals(name, Privileges.toName(option));
    }
  }
}
