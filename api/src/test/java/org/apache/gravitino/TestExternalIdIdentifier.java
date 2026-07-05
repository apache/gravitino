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
package org.apache.gravitino;

import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExternalIdIdentifier {

  private static final String METALAKE = "metalake";

  @Test
  void testOfUser() {
    ExternalIdIdentifier ident = ExternalIdIdentifier.ofUser(METALAKE, "ext-1");
    Assertions.assertEquals(Namespace.of(METALAKE, "system", "user"), ident.namespace());
    Assertions.assertEquals("ext-1", ident.externalId());
    Assertions.assertEquals(METALAKE, ident.metalake());
    Assertions.assertNotEquals(NameIdentifier.of(METALAKE, "system", "user", "ext-1"), ident);
  }

  @Test
  void testInvalidExternalId() {
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> ExternalIdIdentifier.ofUser(METALAKE, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> ExternalIdIdentifier.ofUser(METALAKE, ""));
  }
}
