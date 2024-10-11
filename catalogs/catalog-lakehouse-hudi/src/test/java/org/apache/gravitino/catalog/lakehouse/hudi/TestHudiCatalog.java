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

import static org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata.CATALOG_BACKEND;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHudiCatalog {
  @Test
  public void testOps() throws IOException {
    try (HudiCatalog catalog = new HudiCatalog()) {
      IllegalArgumentException exception =
          Assertions.assertThrows(IllegalArgumentException.class, catalog::ops);
      Assertions.assertEquals(
          "entity and conf must be set before calling ops()", exception.getMessage());
    }

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(HudiCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-hudi")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = ImmutableMap.of(CATALOG_BACKEND, "hms");
    try (HudiCatalog catalog = new HudiCatalog().withCatalogConf(conf).withCatalogEntity(entity)) {
      CatalogOperations ops = catalog.ops();
      Assertions.assertInstanceOf(HudiCatalogOperations.class, ops);
    }
  }
}
