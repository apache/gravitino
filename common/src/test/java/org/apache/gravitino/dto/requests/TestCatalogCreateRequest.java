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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogCreateRequest {

  @Test
  public void testCatalogCreateRequestSerDe() throws JsonProcessingException {
    CatalogCreateRequest request =
        new CatalogCreateRequest(
            "catalog_test",
            Catalog.Type.MODEL,
            "provider_test",
            "catalog comment",
            ImmutableMap.of("key", "value"));

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    CatalogCreateRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, CatalogCreateRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("catalog_test", deserRequest.getName());
    Assertions.assertEquals(Catalog.Type.MODEL, deserRequest.getType());
    Assertions.assertEquals("provider_test", deserRequest.getProvider());
    Assertions.assertEquals("catalog comment", deserRequest.getComment());
    Assertions.assertEquals(ImmutableMap.of("key", "value"), deserRequest.getProperties());

    // Test with null provider, comment and properties
    CatalogCreateRequest request1 =
        new CatalogCreateRequest("catalog_test", Catalog.Type.MODEL, null, null, null);

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    CatalogCreateRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, CatalogCreateRequest.class);
    Assertions.assertEquals(
        deserRequest1.getType().name().toLowerCase(Locale.ROOT), deserRequest1.getProvider());
    Assertions.assertNull(deserRequest1.getComment());
    Assertions.assertNull(deserRequest1.getProperties());

    String json = "{\"name\":\"catalog_test\",\"type\":\"model\"}";
    CatalogCreateRequest deserRequest2 =
        JsonUtils.objectMapper().readValue(json, CatalogCreateRequest.class);
    Assertions.assertEquals("model", deserRequest2.getProvider());

    // Test using null provider with catalog type doesn't support managed catalog
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new CatalogCreateRequest("catalog_test", Catalog.Type.RELATIONAL, null, null, null));

    String json1 = "{\"name\":\"catalog_test\",\"type\":\"relational\"}";
    Assertions.assertThrows(
        JsonMappingException.class,
        () -> JsonUtils.objectMapper().readValue(json1, CatalogCreateRequest.class));
  }

  @Test
  public void testCatalogCreateRequestNullTypeHadoopProvider() {
    // Test NPE case when type is null and provider is "hadoop"
    // This should throw IllegalArgumentException instead of NPE in constructor
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new CatalogCreateRequest("catalog_test", null, "hadoop", null, null));

    // Verify the exception message
    Assertions.assertEquals(
        "Catalog type cannot be null when provider is \"hadoop\"", exception.getMessage());
  }
}
