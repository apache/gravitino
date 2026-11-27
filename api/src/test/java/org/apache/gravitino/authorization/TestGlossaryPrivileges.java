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

class TestGlossaryPrivileges {
  @Test
  void testAllowedGrantScopes() {
    for (Privilege.Name name : Privilege.Name.values()) {
      for (Privilege privilege : new Privilege[] {Privileges.allow(name), Privileges.deny(name)}) {
        Assertions.assertFalse(privilege.canBindTo(MetadataObject.Type.GLOSSARY_TERM), name::name);
        if (name == Privilege.Name.VIEW_DEFINITION
            || name == Privilege.Name.CREATE_DEFINITION
            || name == Privilege.Name.MANAGE_DEFINITIONS) {
          for (MetadataObject.Type type : MetadataObject.Type.values()) {
            Assertions.assertEquals(
                type == MetadataObject.Type.METALAKE || type == MetadataObject.Type.DOMAIN,
                privilege.canBindTo(type));
          }
        }
      }
    }
  }
}
