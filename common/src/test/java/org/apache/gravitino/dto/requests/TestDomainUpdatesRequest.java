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
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainUpdatesRequest {

  @Test
  public void testRenameDomainRequestSerDe() throws JsonProcessingException {
    DomainUpdateRequest.RenameDomainRequest request =
        new DomainUpdateRequest.RenameDomainRequest("domain_test_new");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    DomainUpdateRequest.RenameDomainRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, DomainUpdateRequest.RenameDomainRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("domain_test_new", deserRequest.getNewName());
  }

  @Test
  public void testUpdateDomainCommentRequestSerDe() throws JsonProcessingException {
    DomainUpdateRequest.UpdateDomainCommentRequest request =
        new DomainUpdateRequest.UpdateDomainCommentRequest("domain comment new");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    DomainUpdateRequest.UpdateDomainCommentRequest deserRequest =
        JsonUtils.objectMapper()
            .readValue(serJson, DomainUpdateRequest.UpdateDomainCommentRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("domain comment new", deserRequest.getNewComment());
  }

  @Test
  public void testSetDomainPropertyRequestSerDe() throws JsonProcessingException {
    DomainUpdateRequest.SetDomainPropertyRequest request =
        new DomainUpdateRequest.SetDomainPropertyRequest("key", "value");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    DomainUpdateRequest.SetDomainPropertyRequest deserRequest =
        JsonUtils.objectMapper()
            .readValue(serJson, DomainUpdateRequest.SetDomainPropertyRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("key", deserRequest.getProperty());
    Assertions.assertEquals("value", deserRequest.getValue());
  }

  @Test
  public void testRemoveDomainPropertyRequestSerDe() throws JsonProcessingException {
    DomainUpdateRequest.RemoveDomainPropertyRequest request =
        new DomainUpdateRequest.RemoveDomainPropertyRequest("key");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    DomainUpdateRequest.RemoveDomainPropertyRequest deserRequest =
        JsonUtils.objectMapper()
            .readValue(serJson, DomainUpdateRequest.RemoveDomainPropertyRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("key", deserRequest.getProperty());
  }

  @Test
  public void testDomainUpdatesRequestSerDe() throws JsonProcessingException {
    DomainUpdateRequest request = new DomainUpdateRequest.RenameDomainRequest("domain_test_new");
    DomainUpdateRequest request1 =
        new DomainUpdateRequest.UpdateDomainCommentRequest("domain comment new");
    DomainUpdateRequest request2 = new DomainUpdateRequest.SetDomainPropertyRequest("key", "value");
    DomainUpdateRequest request3 = new DomainUpdateRequest.RemoveDomainPropertyRequest("key");

    List<DomainUpdateRequest> updates = ImmutableList.of(request, request1, request2, request3);
    DomainUpdatesRequest domainUpdatesRequest = new DomainUpdatesRequest(updates);
    String serJson = JsonUtils.objectMapper().writeValueAsString(domainUpdatesRequest);
    DomainUpdatesRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, DomainUpdatesRequest.class);
    Assertions.assertEquals(domainUpdatesRequest, deserRequest);
    Assertions.assertEquals(updates, deserRequest.getUpdates());
  }
}
