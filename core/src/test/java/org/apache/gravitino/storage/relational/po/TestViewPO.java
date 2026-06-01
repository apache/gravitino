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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestViewPO {

  @Test
  public void testViewPOBuilder() {
    ViewVersionInfoPO viewVersionInfoPO = buildViewVersionInfoPO(1L, 1);
    ViewPO viewPO =
        ViewPO.builder()
            .withViewId(1L)
            .withViewName("test_view")
            .withMetalakeId(1L)
            .withCatalogId(2L)
            .withSchemaId(3L)
            .withAuditInfo("audit-info")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .withViewVersionInfoPO(viewVersionInfoPO)
            .build();

    Assertions.assertEquals(1L, viewPO.getViewId());
    Assertions.assertEquals("test_view", viewPO.getViewName());
    Assertions.assertEquals(1L, viewPO.getMetalakeId());
    Assertions.assertEquals(2L, viewPO.getCatalogId());
    Assertions.assertEquals(3L, viewPO.getSchemaId());
    Assertions.assertEquals("audit-info", viewPO.getAuditInfo());
    Assertions.assertEquals(1L, viewPO.getCurrentVersion());
    Assertions.assertEquals(1L, viewPO.getLastVersion());
    Assertions.assertEquals(0L, viewPO.getDeletedAt());
    Assertions.assertEquals(viewVersionInfoPO, viewPO.getViewVersionInfoPO());
  }

  @Test
  public void testEqualsAndHashCodeIgnoreViewVersionInfoPO() {
    ViewPO viewPOWithVersionOne =
        ViewPO.builder()
            .withViewId(1L)
            .withViewName("test_view")
            .withMetalakeId(1L)
            .withCatalogId(2L)
            .withSchemaId(3L)
            .withAuditInfo("audit-info")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .withViewVersionInfoPO(buildViewVersionInfoPO(1L, 1))
            .build();

    ViewPO viewPOWithVersionTwo =
        ViewPO.builder()
            .withViewId(1L)
            .withViewName("test_view")
            .withMetalakeId(1L)
            .withCatalogId(2L)
            .withSchemaId(3L)
            .withAuditInfo("audit-info")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .withViewVersionInfoPO(buildViewVersionInfoPO(2L, 2))
            .build();

    Assertions.assertEquals(viewPOWithVersionOne, viewPOWithVersionTwo);
    Assertions.assertEquals(viewPOWithVersionOne.hashCode(), viewPOWithVersionTwo.hashCode());
  }

  @Test
  public void testRepresentationsSerDe() throws JsonProcessingException {
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("spark").withSql("SELECT 1").build(),
          SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build()
        };

    String json = ViewPO.serializeRepresentations(representations);
    Representation[] deserialized = ViewPO.deserializeRepresentations(json);

    Assertions.assertArrayEquals(representations, deserialized);
  }

  @Test
  public void testDeserializeRepresentationsWithoutType() throws JsonProcessingException {
    String json = "[{\"dialect\":\"spark\",\"sql\":\"SELECT 1\"}]";

    Representation[] deserialized = ViewPO.deserializeRepresentations(json);

    Assertions.assertEquals(1, deserialized.length);
    Assertions.assertTrue(deserialized[0] instanceof SQLRepresentation);
    SQLRepresentation sqlRepresentation = (SQLRepresentation) deserialized[0];
    Assertions.assertEquals("spark", sqlRepresentation.dialect());
    Assertions.assertEquals("SELECT 1", sqlRepresentation.sql());
  }

  private ViewVersionInfoPO buildViewVersionInfoPO(Long id, Integer version) {
    return ViewVersionInfoPO.builder()
        .withId(id)
        .withMetalakeId(1L)
        .withCatalogId(2L)
        .withSchemaId(3L)
        .withViewId(1L)
        .withVersion(version)
        .withColumns("[]")
        .withRepresentations("[{\"dialect\":\"spark\",\"sql\":\"SELECT 1\"}]")
        .withAuditInfo("audit-info")
        .withDeletedAt(0L)
        .build();
  }
}
