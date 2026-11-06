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

import org.apache.gravitino.MetadataObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCatalogCredentialPrivileges {
  @Test
  void testFactoriesAndBindings() {
    for (Privilege.Name name :
        new Privilege.Name[] {
          Privilege.Name.USE_SECRETS, Privilege.Name.INCLUDE_CREDENTIAL_SECRETS
        }) {
      Privilege allow = Privileges.allow(name.name());
      Privilege deny = Privileges.deny(name.name());
      Assertions.assertEquals(name, allow.name());
      Assertions.assertEquals(name, deny.name());
      Assertions.assertEquals(Privilege.Condition.ALLOW, allow.condition());
      Assertions.assertEquals(Privilege.Condition.DENY, deny.condition());
      for (MetadataObject.Type type : MetadataObject.Type.values()) {
        boolean supported =
            type == MetadataObject.Type.METALAKE || type == MetadataObject.Type.CATALOG;
        Assertions.assertEquals(supported, allow.canBindTo(type));
        Assertions.assertEquals(supported, deny.canBindTo(type));
      }
    }
  }

  @Test
  void testBitsMatchMainBranch() {
    Assertions.assertEquals(1L << 37, Privilege.Name.USE_SECRETS.getLowBits());
    Assertions.assertEquals(1L << 38, Privilege.Name.INCLUDE_CREDENTIAL_SECRETS.getLowBits());
    Assertions.assertEquals(0L, Privilege.Name.USE_SECRETS.getHighBits());
    Assertions.assertEquals(0L, Privilege.Name.INCLUDE_CREDENTIAL_SECRETS.getHighBits());
  }
}
