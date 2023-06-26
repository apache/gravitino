package com.datastrato.graviton.json;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.dto.requests.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRequestJsonSerDe {

  @Test
  public void testLakehouseCreateRequestSerDe() throws JsonProcessingException {
    LakehouseCreateRequest request =
        new LakehouseCreateRequest("lakehouse", "comment", ImmutableMap.of("key", "value"));
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    LakehouseCreateRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, LakehouseCreateRequest.class);
    Assertions.assertEquals(request, deserRequest);

    // Test with optional fields
    LakehouseCreateRequest request1 = new LakehouseCreateRequest("lakehouse", null, null);
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    LakehouseCreateRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, LakehouseCreateRequest.class);
    Assertions.assertEquals(request1, deserRequest1);

    LakehouseCreateRequest request2 = new LakehouseCreateRequest("lakehouse", "", null);
    String serJson2 = JsonUtils.objectMapper().writeValueAsString(request2);
    LakehouseCreateRequest deserRequest2 =
        JsonUtils.objectMapper().readValue(serJson2, LakehouseCreateRequest.class);
    Assertions.assertEquals(request2, deserRequest2);

    LakehouseCreateRequest request3 =
        new LakehouseCreateRequest("lakehouse", "", ImmutableMap.of());
    String serJson3 = JsonUtils.objectMapper().writeValueAsString(request3);
    LakehouseCreateRequest deserRequest3 =
        JsonUtils.objectMapper().readValue(serJson3, LakehouseCreateRequest.class);
    Assertions.assertEquals(request3, deserRequest3);
  }

  @Test
  public void testLakehouseUpdateRequestSerDe() throws JsonProcessingException {
    LakehouseUpdateRequest req = new LakehouseUpdateRequest.RenameLakehouseRequest("newLakehouse");
    String serJson = JsonUtils.objectMapper().writeValueAsString(req);
    LakehouseUpdateRequest deserReq =
        JsonUtils.objectMapper().readValue(serJson, LakehouseUpdateRequest.class);
    Assertions.assertEquals(req, deserReq);

    LakehouseUpdateRequest req1 =
        new LakehouseUpdateRequest.UpdateLakehouseCommentRequest("newComment");
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(req1);
    LakehouseUpdateRequest deserReq1 =
        JsonUtils.objectMapper().readValue(serJson1, LakehouseUpdateRequest.class);
    Assertions.assertEquals(req1, deserReq1);

    LakehouseUpdateRequest req2 =
        new LakehouseUpdateRequest.SetLakehousePropertyRequest("key", "value");
    String serJson2 = JsonUtils.objectMapper().writeValueAsString(req2);
    LakehouseUpdateRequest deserReq2 =
        JsonUtils.objectMapper().readValue(serJson2, LakehouseUpdateRequest.class);
    Assertions.assertEquals(req2, deserReq2);

    LakehouseUpdateRequest req3 = new LakehouseUpdateRequest.RemoveLakehousePropertyRequest("key");
    String serJson3 = JsonUtils.objectMapper().writeValueAsString(req3);
    LakehouseUpdateRequest deserReq3 =
        JsonUtils.objectMapper().readValue(serJson3, LakehouseUpdateRequest.class);
    Assertions.assertEquals(req3, deserReq3);

    LakehouseUpdatesRequest req4 =
        new LakehouseUpdatesRequest(ImmutableList.of(req, req1, req2, req3));
    String serJson4 = JsonUtils.objectMapper().writeValueAsString(req4);
    LakehouseUpdatesRequest deserReq4 =
        JsonUtils.objectMapper().readValue(serJson4, LakehouseUpdatesRequest.class);
    Assertions.assertEquals(req4, deserReq4);
  }

  @Test
  public void testCatalogCreateRequestSerDe() throws JsonProcessingException {
    CatalogCreateRequest req =
        new CatalogCreateRequest(
            "catalog", Catalog.Type.RELATIONAL, "comment", ImmutableMap.of("key", "value"));
    String serJson = JsonUtils.objectMapper().writeValueAsString(req);
    CatalogCreateRequest deserReq =
        JsonUtils.objectMapper().readValue(serJson, CatalogCreateRequest.class);
    Assertions.assertEquals(req, deserReq);

    // Test with optional fields
    CatalogCreateRequest req1 =
        new CatalogCreateRequest("catalog", Catalog.Type.RELATIONAL, null, null);
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(req1);
    CatalogCreateRequest deserReq1 =
        JsonUtils.objectMapper().readValue(serJson1, CatalogCreateRequest.class);
    Assertions.assertEquals(req1, deserReq1);

    CatalogCreateRequest req2 =
        new CatalogCreateRequest("catalog", Catalog.Type.RELATIONAL, "", null);
    String serJson2 = JsonUtils.objectMapper().writeValueAsString(req2);
    CatalogCreateRequest deserReq2 =
        JsonUtils.objectMapper().readValue(serJson2, CatalogCreateRequest.class);
    Assertions.assertEquals(req2, deserReq2);

    CatalogCreateRequest req3 =
        new CatalogCreateRequest("catalog", Catalog.Type.RELATIONAL, "", ImmutableMap.of());
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
