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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.dto.domain.DomainBackingTagDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.tag.TagValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainRequests {

  @Test
  public void testCreateRoundTrip() throws IOException {
    for (boolean create : new boolean[] {false, true}) {
      DomainCreateRequest request =
          new DomainCreateRequest(
              "Finance",
              "comment",
              Collections.singletonMap("k", "v"),
              "Parent",
              new DomainCreateRequest.BackingTag("tag", create ? null : "value", create));
      DomainCreateRequest result = roundTrip(request, DomainCreateRequest.class);
      result.validate();
      Assertions.assertEquals("Finance", result.getName());
      Assertions.assertEquals("comment", result.getComment());
      Assertions.assertEquals(Collections.singletonMap("k", "v"), result.getProperties());
      Assertions.assertEquals("Parent", result.getParent());
      Assertions.assertEquals(create, result.getBackingTag().isCreate());
      Assertions.assertEquals(
          create ? TagValue.noValue("tag") : TagValue.of("tag", "value"),
          result.getBackingTag().tagValue());
    }
    DomainCreateRequest existing =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"Finance\",\"backingTag\":{\"name\":\"tag\"}}",
                DomainCreateRequest.class);
    existing.validate();
    Assertions.assertFalse(existing.getBackingTag().isCreate());
    Assertions.assertEquals(TagValue.noValue("tag"), existing.getBackingTag().tagValue());
  }

  @Test
  public void testBackingTagValidation() {
    for (DomainBackingTagDTO invalid :
        new DomainBackingTagDTO[] {
          new DomainBackingTagDTO(), new DomainBackingTagDTO(" ", null),
          new DomainBackingTagDTO("tag", " "),
              new DomainBackingTagDTO("tag", String.join("", Collections.nCopies(257, "x"))),
          new DomainCreateRequest.BackingTag("tag", "value", true)
        }) {
      Assertions.assertThrows(IllegalArgumentException.class, invalid::validate);
      Assertions.assertThrows(IllegalArgumentException.class, invalid::tagValue);
    }
    new DomainBackingTagDTO("tag", String.join("", Collections.nCopies(256, "x"))).validate();
    Assertions.assertEquals(
        TagValue.noValue("tag"), new DomainBackingTagDTO("tag", null).tagValue());
  }

  @Test
  public void testSetRequests() throws IOException {
    DomainSetRequest status =
        roundTrip(new DomainSetRequest(Domain.Status.PUBLISHED), DomainSetRequest.class);
    status.validate();
    Assertions.assertEquals(Domain.Status.PUBLISHED, status.getStatus());
    DomainParentSetRequest parent =
        roundTrip(new DomainParentSetRequest("Parent/US"), DomainParentSetRequest.class);
    parent.validate();
    Assertions.assertEquals("Parent/US", parent.getParent());
    for (DomainBackingTagDTO tag :
        new DomainBackingTagDTO[] {null, new DomainBackingTagDTO("tag", "value")}) {
      DomainBackingTagSetRequest request =
          roundTrip(new DomainBackingTagSetRequest(tag), DomainBackingTagSetRequest.class);
      request.validate();
      if (tag == null) {
        Assertions.assertNull(request.getBackingTag());
      } else {
        Assertions.assertEquals(TagValue.of("tag", "value"), request.getBackingTag().tagValue());
      }
    }
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new DomainBackingTagSetRequest().validate());
  }

  @Test
  public void testRequestValidation() {
    for (RESTRequest invalid :
        new RESTRequest[] {
          new DomainCreateRequest(),
          new DomainCreateRequest(
              " ", null, null, null, new DomainCreateRequest.BackingTag("tag", null, false)),
          new DomainCreateRequest(
              "Finance", null, null, " ", new DomainCreateRequest.BackingTag("tag", null, false)),
          new DomainCreateRequest(
              "Finance",
              null,
              Collections.singletonMap("key", null),
              null,
              new DomainCreateRequest.BackingTag("tag", null, false)),
          new DomainSetRequest(),
          new DomainParentSetRequest(),
          new DomainParentSetRequest(" "),
          new DomainBackingTagSetRequest(new DomainBackingTagDTO()),
          new DomainUpdatesRequest(),
          new DomainUpdatesRequest(Arrays.asList((DomainUpdateRequest) null)),
          new DomainUpdateRequest.RenameDomainRequest(),
          new DomainUpdateRequest.RenameDomainRequest(" "),
          new DomainUpdateRequest.SetDomainPropertyRequest(),
          new DomainUpdateRequest.SetDomainPropertyRequest("key", null),
          new DomainUpdateRequest.RemoveDomainPropertyRequest(),
          new DomainUpdateRequest.RemoveDomainPropertyRequest(" ")
        }) {
      Assertions.assertThrows(IllegalArgumentException.class, invalid::validate);
    }
    new DomainUpdatesRequest(Collections.emptyList()).validate();
    DomainUpdateRequest.SetDomainPropertyRequest emptyValue =
        new DomainUpdateRequest.SetDomainPropertyRequest("key", "");
    emptyValue.validate();
    Assertions.assertEquals(DomainChange.setProperty("key", ""), emptyValue.domainChange());
    DomainUpdateRequest.UpdateDomainCommentRequest clearComment =
        new DomainUpdateRequest.UpdateDomainCommentRequest();
    clearComment.validate();
    Assertions.assertEquals(DomainChange.updateComment(null), clearComment.domainChange());
    Assertions.assertEquals(
        DomainChange.rename("New"),
        new DomainUpdateRequest.RenameDomainRequest("New").domainChange());
    Assertions.assertEquals(
        DomainChange.removeProperty("key"),
        new DomainUpdateRequest.RemoveDomainPropertyRequest("key").domainChange());
  }

  private static <T> T roundTrip(T value, Class<T> type) throws IOException {
    return JsonUtils.objectMapper()
        .readValue(JsonUtils.objectMapper().writeValueAsString(value), type);
  }
}
