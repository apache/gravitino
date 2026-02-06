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
package org.apache.gravitino.storage.relational.po;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFunctionPO {

  @Test
  public void testFunctionPOBuilder() {
    FunctionPO functionPO =
        FunctionPO.builder()
            .withFunctionId(1L)
            .withFunctionName("test-function")
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withFunctionType("SCALAR")
            .withDeterministic(1)
            .withFunctionLatestVersion(1)
            .withFunctionCurrentVersion(1)
            .withAuditInfo("audit-info")
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(1L, functionPO.functionId());
    Assertions.assertEquals("test-function", functionPO.functionName());
    Assertions.assertEquals(1L, functionPO.metalakeId());
    Assertions.assertEquals(1L, functionPO.catalogId());
    Assertions.assertEquals(1L, functionPO.schemaId());
    Assertions.assertEquals("SCALAR", functionPO.functionType());
    Assertions.assertEquals(1, functionPO.deterministic());
    Assertions.assertEquals(1, functionPO.functionLatestVersion());
    Assertions.assertEquals(1, functionPO.functionCurrentVersion());
    Assertions.assertEquals("audit-info", functionPO.auditInfo());
    Assertions.assertEquals(0L, functionPO.deletedAt());
  }

  @Test
  public void testFunctionVersionPOBuilder() {
    FunctionVersionPO functionVersionPO =
        FunctionVersionPO.builder()
            .withId(1L)
            .withFunctionId(1L)
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withFunctionVersion(1)
            .withFunctionComment("test-comment")
            .withDefinitions("definitions")
            .withAuditInfo("audit-info")
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(1L, functionVersionPO.id());
    Assertions.assertEquals(1L, functionVersionPO.functionId());
    Assertions.assertEquals(1L, functionVersionPO.metalakeId());
    Assertions.assertEquals(1L, functionVersionPO.catalogId());
    Assertions.assertEquals(1L, functionVersionPO.schemaId());
    Assertions.assertEquals(1, functionVersionPO.functionVersion());
    Assertions.assertEquals("test-comment", functionVersionPO.functionComment());
    Assertions.assertEquals("definitions", functionVersionPO.definitions());
    Assertions.assertEquals("audit-info", functionVersionPO.auditInfo());
    Assertions.assertEquals(0L, functionVersionPO.deletedAt());
  }

  @Test
  public void testFunctionMaxVersionPOBuilder() {
    FunctionMaxVersionPO functionMaxVersionPO =
        FunctionMaxVersionPO.builder().withFunctionId(1L).withVersion(1L).build();

    Assertions.assertEquals(1L, functionMaxVersionPO.functionId());
    Assertions.assertEquals(1L, functionMaxVersionPO.version());
  }

  @Test
  public void testEqualsAndHashCode() {
    FunctionPO functionPO1 =
        FunctionPO.builder()
            .withFunctionId(1L)
            .withFunctionName("test-function")
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withFunctionType("SCALAR")
            .withDeterministic(1)
            .withFunctionLatestVersion(1)
            .withFunctionCurrentVersion(1)
            .withAuditInfo("audit-info")
            .withDeletedAt(0L)
            .build();

    FunctionPO functionPO2 =
        FunctionPO.builder()
            .withFunctionId(1L)
            .withFunctionName("test-function")
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withFunctionType("SCALAR")
            .withDeterministic(1)
            .withFunctionLatestVersion(1)
            .withFunctionCurrentVersion(1)
            .withAuditInfo("audit-info")
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(functionPO1, functionPO2);
    Assertions.assertEquals(functionPO1.hashCode(), functionPO2.hashCode());

    FunctionVersionPO functionVersionPO1 =
        FunctionVersionPO.builder()
            .withId(1L)
            .withFunctionId(1L)
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withFunctionVersion(1)
            .withFunctionComment("test-comment")
            .withDefinitions("definitions")
            .withAuditInfo("audit-info")
            .withDeletedAt(0L)
            .build();

    FunctionVersionPO functionVersionPO2 =
        FunctionVersionPO.builder()
            .withId(1L)
            .withFunctionId(1L)
            .withMetalakeId(1L)
            .withCatalogId(1L)
            .withSchemaId(1L)
            .withFunctionVersion(1)
            .withFunctionComment("test-comment")
            .withDefinitions("definitions")
            .withAuditInfo("audit-info")
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(functionVersionPO1, functionVersionPO2);
    Assertions.assertEquals(functionVersionPO1.hashCode(), functionVersionPO2.hashCode());

    FunctionMaxVersionPO functionMaxVersionPO1 =
        FunctionMaxVersionPO.builder().withFunctionId(1L).withVersion(1L).build();

    FunctionMaxVersionPO functionMaxVersionPO2 =
        FunctionMaxVersionPO.builder().withFunctionId(1L).withVersion(1L).build();

    Assertions.assertEquals(functionMaxVersionPO1, functionMaxVersionPO2);
    Assertions.assertEquals(functionMaxVersionPO1.hashCode(), functionMaxVersionPO2.hashCode());
  }
}
