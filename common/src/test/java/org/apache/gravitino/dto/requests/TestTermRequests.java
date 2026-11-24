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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import java.util.Collections;
import org.apache.gravitino.dto.responses.TermListResponse;
import org.apache.gravitino.dto.responses.TermResponse;
import org.apache.gravitino.dto.responses.TermTreeResponse;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.TermChange;
import org.apache.gravitino.glossary.TermLink;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Test;

class TestTermRequests {
  private final ObjectMapper mapper = JsonUtils.objectMapper();

  @Test
  void testCreationAndDefaults() throws Exception {
    String json =
        "{\"name\":\"Example\",\"definition\":\"Glossary definition\",\"domains\":[\"Sales\"],\"broader\":[\"Parent\"],\"related\":[\"Peer\"],\"synonyms\":[\"Syn\"],\"properties\":{\"p\":\"value\"},\"status\":\"APPROVED\",\"links\":[{\"label\":\"docs\",\"url\":\"https://example.org\"}],\"sourceRef\":\"source:id\"}";
    TermCreateRequest request = roundTrip(json, TermCreateRequest.class);
    assertEquals(
        TermSpec.builder("Example")
            .withDefinition("Glossary definition")
            .withDomains("Sales")
            .withBroader("Parent")
            .withRelated("Peer")
            .withSynonyms("Syn")
            .withProperties(Collections.singletonMap("p", "value"))
            .withStatus(GlossaryTerm.Status.APPROVED)
            .withLinks(TermLink.of("docs", "https://example.org"))
            .withSourceRef("source:id")
            .build(),
        request.toSpec());
    assertEquals(
        TermSpec.builder("name").build(),
        mapper.readValue("{\"name\":\"name\"}", TermCreateRequest.class).toSpec());
  }

  @Test
  void testOrderedUpdates() throws Exception {
    String json =
        "{\"expectedRevision\":\"token\",\"updates\":["
            + "{\"@type\":\"rename\",\"newName\":\"new\"}"
            + ","
            + "{\"@type\":\"updateDefinition\",\"newDefinition\":\"text\"}"
            + ","
            + "{\"@type\":\"setStatus\",\"status\":\"DRAFT\"}"
            + ","
            + "{\"@type\":\"addSynonym\",\"synonym\":\"a\"}"
            + ","
            + "{\"@type\":\"removeSynonym\",\"synonym\":\"a\"}"
            + ","
            + "{\"@type\":\"addDomain\",\"domain\":\"a\"}"
            + ","
            + "{\"@type\":\"removeDomain\",\"domain\":\"a\"}"
            + ","
            + "{\"@type\":\"addBroader\",\"broader\":\"a\"}"
            + ","
            + "{\"@type\":\"removeBroader\",\"broader\":\"a\"}"
            + ","
            + "{\"@type\":\"addRelated\",\"related\":\"a\"}"
            + ","
            + "{\"@type\":\"removeRelated\",\"related\":\"a\"}"
            + ","
            + "{\"@type\":\"updateLinks\",\"links\":[{\"label\":\"docs\",\"url\":\"https://example.org\"}]}"
            + ","
            + "{\"@type\":\"setSourceRef\",\"sourceRef\":\"source:id\"}"
            + ","
            + "{\"@type\":\"setProperty\",\"property\":\"x\",\"value\":\"y\"}"
            + ","
            + "{\"@type\":\"removeProperty\",\"property\":\"x\"}"
            + "]}";
    TermUpdatesRequest request = roundTrip(json, TermUpdatesRequest.class);
    assertEquals("token", request.getExpectedRevision());
    assertArrayEquals(
        new TermChange[] {
          TermChange.rename("new"),
          TermChange.updateDefinition("text"),
          TermChange.setStatus(GlossaryTerm.Status.DRAFT),
          TermChange.addSynonym("a"),
          TermChange.removeSynonym("a"),
          TermChange.addDomain("a"),
          TermChange.removeDomain("a"),
          TermChange.addBroader("a"),
          TermChange.removeBroader("a"),
          TermChange.addRelated("a"),
          TermChange.removeRelated("a"),
          TermChange.updateLinks(new TermLink[] {TermLink.of("docs", "https://example.org")}),
          TermChange.setSourceRef("source:id"),
          TermChange.setProperty("x", "y"),
          TermChange.removeProperty("x")
        },
        request.changes());
  }

  @Test
  void testClearRequiresExplicitNull() throws Exception {
    for (String[] clear :
        new String[][] {{"updateDefinition", "newDefinition"}, {"setSourceRef", "sourceRef"}}) {
      String json = "{\"updates\":[{\"@type\":\"" + clear[0] + "\",\"" + clear[1] + "\":null}]}";
      TermUpdatesRequest decoded = mapper.readValue(json, TermUpdatesRequest.class);
      assertEquals(
          mapper.readTree(json).get("updates"),
          mapper.readTree(mapper.writeValueAsString(decoded)).get("updates"));
      assertThrows(
          MismatchedInputException.class,
          () ->
              mapper.readValue(
                  "{\"updates\":[{\"@type\":\"" + clear[0] + "\"}]}", TermUpdatesRequest.class));
    }
  }

  @Test
  void testOmittedRevisionAndExplicitNullAreDistinct() throws Exception {
    TermUpdatesRequest unconditional =
        new TermUpdatesRequest(null, new TermUpdateRequest.Rename("New"));
    assertFalse(mapper.readTree(mapper.writeValueAsBytes(unconditional)).has("expectedRevision"));
    assertThrows(
        MismatchedInputException.class,
        () ->
            mapper.readValue(
                "{\"expectedRevision\":null,\"updates\":[{\"@type\":\"rename\",\"newName\":\"New\"}]}",
                TermUpdatesRequest.class));
  }

  @Test
  void testProgrammaticRequests() throws Exception {
    TermSpec spec = TermSpec.builder("Example").withBroader("Parent").build();
    assertEquals(
        spec,
        mapper
            .readValue(
                mapper.writeValueAsString(new TermCreateRequest(spec)), TermCreateRequest.class)
            .toSpec());
    TermUpdatesRequest updates = new TermUpdatesRequest(null, new TermUpdateRequest.Rename("New"));
    assertArrayEquals(
        new TermChange[] {TermChange.rename("New")},
        mapper.readValue(mapper.writeValueAsString(updates), TermUpdatesRequest.class).changes());
  }

  @Test
  void testResponses() throws Exception {
    String term =
        "{\"name\":\"Example\",\"definition\":\"Glossary definition\",\"domains\":[\"Sales\"],\"broader\":[\"Parent\"],\"related\":[\"Peer\"],\"synonyms\":[\"Syn\"],\"properties\":{\"p\":\"value\"},\"status\":\"APPROVED\",\"links\":[{\"label\":\"docs\",\"url\":\"https://example.org\"}],\"sourceRef\":\"source:id\",\"narrower\":[\"Child\"],\"version\":2,\"revisionToken\":\"opaque\",\"owner\":{\"name\":\"alice\",\"type\":\"user\"},\"audit\":{\"creator\":\"alice\",\"createTime\":\"2026-01-01T00:00:00Z\",\"lastModifier\":null,\"lastModifiedTime\":null}}";
    roundTrip("{\"code\":0,\"term\":" + term + "}", TermResponse.class).validate();
    String list = "{\"code\":0,\"terms\":[" + term + "]}";
    roundTrip(list, TermListResponse.class).validate();
    roundTrip(list, TermTreeResponse.class).validate();

    assertThrows(IllegalArgumentException.class, () -> new TermListResponse(null));
    TermListResponse missingTerms = mapper.readValue("{\"code\":0}", TermListResponse.class);
    assertNull(missingTerms.getTerms());
    assertThrows(IllegalArgumentException.class, missingTerms::validate);

    TermUpdatesRequest invalidInner =
        mapper.readValue(
            "{\"updates\":[{\"@type\":\"setStatus\",\"status\":\"BOGUS\"}]}",
            TermUpdatesRequest.class);
    assertThrows(IllegalArgumentException.class, invalidInner::validate);
  }

  private <T> T roundTrip(String json, Class<T> type) throws Exception {
    T decoded = mapper.readValue(json, type);
    assertEquals(mapper.readTree(json), mapper.readTree(mapper.writeValueAsString(decoded)));
    return decoded;
  }
}
