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

class TestDomainPrivileges {

  @Test
  void domainPrivilegesBindOnlyToDomainsAndMetalakes() {
    for (Privilege.Name name :
        new Privilege.Name[] {Privilege.Name.MANAGE_DOMAIN, Privilege.Name.USE_DOMAIN}) {
      for (Privilege privilege : new Privilege[] {Privileges.allow(name), Privileges.deny(name)}) {
        Assertions.assertEquals(name, privilege.name());
        Assertions.assertEquals(
            privilege,
            privilege.condition() == Privilege.Condition.ALLOW
                ? Privileges.allow(name.name())
                : Privileges.deny(name.name()));
        for (MetadataObject.Type type : MetadataObject.Type.values()) {
          Assertions.assertEquals(
              type == MetadataObject.Type.DOMAIN || type == MetadataObject.Type.METALAKE,
              privilege.canBindTo(type));
        }
      }
    }
    Assertions.assertTrue(
        Privileges.allow(Privilege.Name.MANAGE_GRANTS).canBindTo(MetadataObject.Type.DOMAIN));
    Assertions.assertFalse(
        Privileges.allow(Privilege.Name.SELECT_TABLE).canBindTo(MetadataObject.Type.DOMAIN));
    Assertions.assertFalse(
        Privileges.allow(Privilege.Name.APPLY_TAG).canBindTo(MetadataObject.Type.DOMAIN));
  }
}
