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
package org.apache.gravitino.dto.responses;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.dto.glossary.TermBindingDTO;
import org.apache.gravitino.dto.glossary.TermDTO;
import org.apache.gravitino.glossary.TermBindingPage;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Test;

class TestTermBindingResponses {
  private static final ObjectMapper MAPPER = JsonUtils.objectMapper();
  private static final String TABLE = "{\"fullName\":\"dw.sales.orders\",\"type\":\"TABLE\"}";
  private static final String MODEL =
      "{\"fullName\":\"dw.sales.sales\",\"type\":\"SEMANTIC_MODEL\"}";

  @Test
  void testBindingKindsAndPageLimits() throws Exception {
    String table = "{\"kind\":\"IMPLEMENTED_BY\",\"object\":" + TABLE + "}";
    String metric = "{\"kind\":\"USED_BY\",\"metric\":\"units\",\"object\":" + MODEL + "}";
    TermBindingListResponse page = page(table + "," + metric);
    page.validate();
    assertNull(page.getNextPageToken());
    for (String invalid :
        Arrays.asList(
            "null",
            "{}",
            "{\"kind\":\"IMPLEMENTED_BY\"}",
            "{\"kind\":\"IMPLEMENTED_BY\",\"object\":" + MODEL + "}",
            "{\"kind\":\"IMPLEMENTED_BY\",\"metric\":\"units\",\"object\":" + TABLE + "}",
            "{\"kind\":\"USED_BY\",\"object\":" + MODEL + "}",
            "{\"kind\":\"USED_BY\",\"object\":" + TABLE + ",\"metric\":\"units\"}")) {
      assertThrows(IllegalArgumentException.class, page(invalid)::validate, invalid);
    }
    TermBindingDTO[] items = new TermBindingDTO[TermBindingPage.MAX_PAGE_SIZE + 1];
    Arrays.fill(items, page.getBindings()[0]);
    assertThrows(
        IllegalArgumentException.class, new TermBindingListResponse(items, null)::validate);
    assertThrows(
        IllegalArgumentException.class,
        new TermBindingListResponse(new TermBindingDTO[0], " ")::validate);
    new TermBindingListResponse(new TermBindingDTO[0], "continue").validate();
  }

  @Test
  void testDirectAndChildTerms() throws Exception {
    String envelope = "{\"terms\":[],\"children\":[]}";
    ObjectTermsResponse reply = MAPPER.readValue(envelope, ObjectTermsResponse.class);
    reply.validate();
    assertEquals(0, reply.getTerms().length);
    assertThrows(
        IllegalArgumentException.class,
        MAPPER.readValue("{\"terms\":[null],\"children\":[]}", ObjectTermsResponse.class)
            ::validate);
    assertThrows(
        IllegalArgumentException.class,
        MAPPER.readValue("{\"terms\":[],\"children\":[null]}", ObjectTermsResponse.class)
            ::validate);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ObjectTermsResponse.Child("metric", Collections.emptyList()));
    TermDTO term =
        MAPPER.readValue(
            "{\"name\":\"Sales\",\"status\":\"APPROVED\",\"version\":1,\"revisionToken\":\"v\"}",
            TermDTO.class);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ObjectTermsResponse.Child(" ", Collections.singletonList(term)));
    assertThrows(UnsupportedOperationException.class, () -> reply.getChildren().add(null));
  }

  private static TermBindingListResponse page(String items) throws Exception {
    return MAPPER.readValue("{\"bindings\":[" + items + "]}", TermBindingListResponse.class);
  }
}
