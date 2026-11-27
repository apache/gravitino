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
package org.apache.gravitino.catalog.fileset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.connector.capability.Capability.Scope;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestFilesetCatalogCapability {
  private final FilesetCatalogCapability capability = new FilesetCatalogCapability();

  @ParameterizedTest
  @EnumSource(
      value = Scope.class,
      names = {"SCHEMA", "FILESET"})
  void reportsSyntaxErrorsBeforeLength(Scope scope) {
    String slashMessage = "Fileset catalog does not support '/' in the name for " + scope + ".";
    for (int length : new int[] {64, 65}) {
      String name = "a/" + "b".repeat(length - 2);
      assertEquals(slashMessage, capability.specificationOnName(scope, name).unsupportedMessage());

      String illegal = "a/?" + "b".repeat(length - 3);
      assertEquals(
          "The " + scope + " name '" + illegal + "' is illegal.",
          capability.specificationOnName(scope, illegal).unsupportedMessage());
    }

    String maxName = "a".repeat(64);
    assertTrue(capability.specificationOnName(scope, maxName).supported());
    assertEquals(
        "The " + scope + " name must not exceed 64 characters.",
        capability.specificationOnName(scope, maxName + "a").unsupportedMessage());
  }
}
