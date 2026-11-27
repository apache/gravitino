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

import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestOntologyPrivileges {

  @Test
  void testBindingAndParsingOfOntologyPrivileges() {
    for (Privilege.Name name :
        List.of(
            Privilege.Name.CREATE_ONTOLOGY,
            Privilege.Name.USE_ONTOLOGY,
            Privilege.Name.MODIFY_ONTOLOGY)) {
      for (Privilege privilege : List.of(Privileges.allow(name), Privileges.deny(name))) {
        Assertions.assertEquals(name, privilege.name());
        for (MetadataObject.Type type : MetadataObject.Type.values()) {
          boolean allowed =
              type == MetadataObject.Type.METALAKE
                  || (type == MetadataObject.Type.ONTOLOGY
                      && name != Privilege.Name.CREATE_ONTOLOGY);
          Assertions.assertEquals(allowed, privilege.canBindTo(type), name + " on " + type);
        }
      }
    }
    SecurableObject ontology =
        SecurableObjects.ofOntology("Flights", List.of(Privileges.UseOntology.allow()));
    Assertions.assertEquals(MetadataObject.Type.ONTOLOGY, ontology.type());
    Assertions.assertEquals("Flights", ontology.fullName());
    Assertions.assertEquals(Privileges.UseOntology.allow(), ontology.privileges().get(0));
  }
}
