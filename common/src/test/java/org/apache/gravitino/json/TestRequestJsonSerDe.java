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
package org.apache.gravitino.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.MetalakeCreateRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdateRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdatesRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRequestJsonSerDe {

  @Test
  public void testMetalakeCreateRequestSerDe() throws JsonProcessingException {
    MetalakeCreateRequest request =
        new MetalakeCreateRequest("metalake", "comment", ImmutableMap.of("key", "value"));
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    MetalakeCreateRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, MetalakeCreateRequest.class);
    Assertions.assertEquals(request, deserRequest);

    // Test with optional fields
    MetalakeCreateRequest request1 = new MetalakeCreateRequest("Metalake", null, null);
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    MetalakeCreateRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, MetalakeCreateRequest.class);
    Assertions.assertEquals(request1, deserRequest1);

    MetalakeCreateRequest request2 = new MetalakeCreateRequest("Metalake", "", null);
    String serJson2 = JsonUtils.objectMapper().writeValueAsString(request2);
    MetalakeCreateRequest deserRequest2 =
        JsonUtils.objectMapper().readValue(serJson2, MetalakeCreateRequest.class);
    Assertions.assertEquals(request2, deserRequest2);

    MetalakeCreateRequest request3 = new MetalakeCreateRequest("Metalake", "", ImmutableMap.of());
    String serJson3 = JsonUtils.objectMapper().writeValueAsString(request3);
    MetalakeCreateRequest deserRequest3 =
        JsonUtils.objectMapper().readValue(serJson3, MetalakeCreateRequest.class);
    Assertions.assertEquals(request3, deserRequest3);
  }

  @Test
  public void testMetalakeUpdateRequestSerDe() throws JsonProcessingException {
    MetalakeUpdateRequest req = new MetalakeUpdateRequest.RenameMetalakeRequest("newMetalake");
    String serJson = JsonUtils.objectMapper().writeValueAsString(req);
    MetalakeUpdateRequest deserReq =
        JsonUtils.objectMapper().readValue(serJson, MetalakeUpdateRequest.class);
    Assertions.assertEquals(req, deserReq);

    MetalakeUpdateRequest req1 =
        new MetalakeUpdateRequest.UpdateMetalakeCommentRequest("newComment");
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(req1);
    MetalakeUpdateRequest deserReq1 =
        JsonUtils.objectMapper().readValue(serJson1, MetalakeUpdateRequest.class);
    Assertions.assertEquals(req1, deserReq1);

    MetalakeUpdateRequest req2 =
        new MetalakeUpdateRequest.SetMetalakePropertyRequest("key", "value");
    String serJson2 = JsonUtils.objectMapper().writeValueAsString(req2);
    MetalakeUpdateRequest deserReq2 =
        JsonUtils.objectMapper().readValue(serJson2, MetalakeUpdateRequest.class);
    Assertions.assertEquals(req2, deserReq2);

    MetalakeUpdateRequest req3 = new MetalakeUpdateRequest.RemoveMetalakePropertyRequest("key");
    String serJson3 = JsonUtils.objectMapper().writeValueAsString(req3);
    MetalakeUpdateRequest deserReq3 =
        JsonUtils.objectMapper().readValue(serJson3, MetalakeUpdateRequest.class);
    Assertions.assertEquals(req3, deserReq3);

    MetalakeUpdatesRequest req4 =
        new MetalakeUpdatesRequest(ImmutableList.of(req, req1, req2, req3));
    String serJson4 = JsonUtils.objectMapper().writeValueAsString(req4);
    MetalakeUpdatesRequest deserReq4 =
        JsonUtils.objectMapper().readValue(serJson4, MetalakeUpdatesRequest.class);
    Assertions.assertEquals(req4, deserReq4);
  }

  @Test
  public void testCatalogCreateRequestSerDe() throws JsonProcessingException {
    CatalogCreateRequest req =
        new CatalogCreateRequest(
            "catalog", Catalog.Type.RELATIONAL, "hive", "comment", ImmutableMap.of("key", "value"));
    String serJson = JsonUtils.objectMapper().writeValueAsString(req);
    CatalogCreateRequest deserReq =
        JsonUtils.objectMapper().readValue(serJson, CatalogCreateRequest.class);
    Assertions.assertEquals(req, deserReq);

    // Test with optional fields
    CatalogCreateRequest req1 =
        new CatalogCreateRequest("catalog", Catalog.Type.RELATIONAL, "hive", null, null);
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(req1);
    CatalogCreateRequest deserReq1 =
        JsonUtils.objectMapper().readValue(serJson1, CatalogCreateRequest.class);
    Assertions.assertEquals(req1, deserReq1);

    CatalogCreateRequest req2 =
        new CatalogCreateRequest("catalog", Catalog.Type.RELATIONAL, "hive", "", null);
    String serJson2 = JsonUtils.objectMapper().writeValueAsString(req2);
    CatalogCreateRequest deserReq2 =
        JsonUtils.objectMapper().readValue(serJson2, CatalogCreateRequest.class);
    Assertions.assertEquals(req2, deserReq2);

    CatalogCreateRequest req3 =
        new CatalogCreateRequest("catalog", Catalog.Type.RELATIONAL, "hive", "", ImmutableMap.of());
    String serJson3 = JsonUtils.objectMapper().writeValueAsString(req3);
    CatalogCreateRequest deserReq3 =
        JsonUtils.objectMapper().readValue(serJson3, CatalogCreateRequest.class);
    Assertions.assertEquals(req3, deserReq3);
  }

  @Test
  public void testCatalogUpdateRequestSerDe() throws JsonProcessingException {
    CatalogUpdateRequest req = new CatalogUpdateRequest.RenameCatalogRequest("newCatalog");
    String serJson = JsonUtils.objectMapper().writeValueAsString(req);
    CatalogUpdateRequest deserReq =
        JsonUtils.objectMapper().readValue(serJson, CatalogUpdateRequest.class);
    Assertions.assertEquals(req, deserReq);

    CatalogUpdateRequest req1 = new CatalogUpdateRequest.UpdateCatalogCommentRequest("newComment");
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(req1);
    CatalogUpdateRequest deserReq1 =
        JsonUtils.objectMapper().readValue(serJson1, CatalogUpdateRequest.class);
    Assertions.assertEquals(req1, deserReq1);

    CatalogUpdateRequest req2 = new CatalogUpdateRequest.SetCatalogPropertyRequest("key", "value");
    String serJson2 = JsonUtils.objectMapper().writeValueAsString(req2);
    CatalogUpdateRequest deserReq2 =
        JsonUtils.objectMapper().readValue(serJson2, CatalogUpdateRequest.class);
    Assertions.assertEquals(req2, deserReq2);

    CatalogUpdateRequest req3 = new CatalogUpdateRequest.RemoveCatalogPropertyRequest("key");
    String serJson3 = JsonUtils.objectMapper().writeValueAsString(req3);
    CatalogUpdateRequest deserReq3 =
        JsonUtils.objectMapper().readValue(serJson3, CatalogUpdateRequest.class);
    Assertions.assertEquals(req3, deserReq3);
  }
}
