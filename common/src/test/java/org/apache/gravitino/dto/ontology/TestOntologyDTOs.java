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
package org.apache.gravitino.dto.ontology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.Map;
import org.apache.gravitino.dto.requests.OntologyCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.OntologyResponse;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.ontology.OntologyChange;
import org.junit.jupiter.api.Test;

class TestOntologyDTOs {
  private static final ObjectMapper JSON = JsonUtils.objectMapper();
  private static final String DEFINITION =
      "{\"ontology\":[{\"concept\":\"Customer\","
          + "\"type\":\"EntityType\"}],\"annotations\":{\"decimal\":1.234567890123456789}}";

  @Test
  void testNativeDefinitionsRetainJsonNumberPrecision() throws Exception {
    OntologyCreateRequest request =
        JSON.readValue(
            "{\"name\":\"Sales\",\"definition\":" + DEFINITION + "}", OntologyCreateRequest.class);
    request.validate();
    Map<String, Object> definition = request.definition().fields();
    assertEquals(
        Map.of("decimal", new BigDecimal("1.234567890123456789")), definition.get("annotations"));

    OntologyUpdatesRequest updates =
        JSON.readValue(
            "{\"updates\":[{\"@type\":\"replaceDefinition\",\"definition\":"
                + DEFINITION
                + "},"
                + "{\"@type\":\"updateComment\",\"newComment\":null}]}",
            OntologyUpdatesRequest.class);
    updates.validate();
    OntologyChange[] changes = updates.ontologyChanges();
    assertEquals(OntologyChange.replaceDefinition(request.definition()), changes[0]);
    assertEquals(OntologyChange.updateComment(null), changes[1]);

    OntologyResponse response =
        JSON.readValue(
            "{\"code\":0,\"ontology\":{\"name\":\"Sales\",\"definition\":"
                + DEFINITION
                + ",\"properties\":{},\"audit\":{\"creator\":\"steward\"}}}",
            OntologyResponse.class);
    response.validate();
    assertEquals(request.definition(), response.ontology().definition());
    assertEquals("steward", response.ontology().auditInfo().creator());
  }

  @Test
  void testMappingCannotAlterItsOntology() throws Exception {
    OntologyUpdatesRequest request =
        JSON.readValue(
            "{\"updates\":[{\"@type\":\"replaceOntology\"}]}", OntologyUpdatesRequest.class);
    assertThrows(IllegalArgumentException.class, request::mappingChanges);
  }
}
