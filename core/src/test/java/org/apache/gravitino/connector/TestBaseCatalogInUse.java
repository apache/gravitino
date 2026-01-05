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
package org.apache.gravitino.connector;

import static org.apache.gravitino.connector.BaseCatalogPropertiesMetadata.PROPERTY_METALAKE_IN_USE;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBaseCatalogInUse {

  @Test
  void testMetalakeAndCatalogInUse() throws IllegalAccessException {
    CatalogEntity entity = buildCatalogEntity(props(true, true));
    TestCatalogImpl catalog = new TestCatalogImpl();
    FieldUtils.writeField(catalog, "entity", entity, true);
    Assertions.assertDoesNotThrow(() -> catalog.checkMetalakeAndCatalogInUse());
  }

  @Test
  void testMetalakeNotInUseThrows() throws IllegalAccessException {
    CatalogEntity entity = buildCatalogEntity(props(false, true));
    TestCatalogImpl catalog = new TestCatalogImpl();
    FieldUtils.writeField(catalog, "entity", entity, true);
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> catalog.checkMetalakeAndCatalogInUse());
  }

  @Test
  void testCatalogNotInUseThrows() throws IllegalAccessException {
    CatalogEntity entity = buildCatalogEntity(props(true, false));
    TestCatalogImpl catalog = new TestCatalogImpl();
    FieldUtils.writeField(catalog, "entity", entity, true);
    Assertions.assertThrows(
        CatalogNotInUseException.class, () -> catalog.checkMetalakeAndCatalogInUse());
  }

  private Map<String, String> props(boolean metalakeInUse, boolean catalogInUse) {
    Map<String, String> props = new HashMap<>();
    props.put(PROPERTY_METALAKE_IN_USE, String.valueOf(metalakeInUse));
    props.put(Catalog.PROPERTY_IN_USE, String.valueOf(catalogInUse));
    return props;
  }

  private CatalogEntity buildCatalogEntity(Map<String, String> props) {
    return CatalogEntity.builder()
        .withId(1L)
        .withName("test")
        .withNamespace(Namespace.of("metalake"))
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment("comment")
        .withProperties(props)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator("tester")
                .withCreateTime(Instant.now())
                .withLastModifier("tester")
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private static class TestCatalogImpl extends BaseCatalog<TestCatalogImpl> {

    @Override
    public String shortName() {
      return "test";
    }

    @Override
    protected CatalogOperations newOps(Map<String, String> config) {
      return Mockito.mock(CatalogOperations.class);
    }

    @Override
    protected Capability newCapability() {
      return Capability.DEFAULT;
    }

    @Override
    public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
      return BaseCatalogPropertiesMetadata.BASIC_CATALOG_PROPERTIES_METADATA;
    }
  }
}
