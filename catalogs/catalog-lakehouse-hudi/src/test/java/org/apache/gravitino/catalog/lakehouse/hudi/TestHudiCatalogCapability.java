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
package org.apache.gravitino.catalog.lakehouse.hudi;

import org.apache.gravitino.connector.capability.Capability;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link HudiCatalogCapability}. */
public class TestHudiCatalogCapability {

  @Test
  public void testHiveBackendFoldsSchemaAndTableNamesOnly() {
    HudiCatalogCapability capability = new HudiCatalogCapability();

    Assertions.assertFalse(capability.caseSensitiveOnName(Capability.Scope.SCHEMA).supported());
    Assertions.assertFalse(capability.caseSensitiveOnName(Capability.Scope.TABLE).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.VIEW).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.COLUMN).supported());

    Assertions.assertEquals("schema", capability.normalizeName(Capability.Scope.SCHEMA, "Schema"));
    Assertions.assertEquals("table", capability.normalizeName(Capability.Scope.TABLE, "Table"));
    Assertions.assertEquals("View", capability.normalizeName(Capability.Scope.VIEW, "View"));
    Assertions.assertEquals("Column", capability.normalizeName(Capability.Scope.COLUMN, "Column"));
  }
}
