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
package org.apache.gravitino.catalog.kafka;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.catalog.TopicNormalizeDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.Capability.Scope;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.dto.requests.TopicCreateRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.server.web.rest.TopicOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests Kafka topic name validation and its REST error response. */
public class TestKafkaCatalogCapability {

  private final KafkaCatalogCapability capability = new KafkaCatalogCapability();

  @Test
  void testValidTopicNames() {
    for (String name :
        new String[] {
          "a",
          "Topic_123-test",
          "_topic",
          "-topic",
          "-",
          "a".repeat(64),
          "a".repeat(65),
          "a".repeat(128)
        }) {
      Assertions.assertTrue(capability.specificationOnName(Scope.TOPIC, name).supported(), name);
    }
  }

  @Test
  void testKafkaIllegalTopicNames() {
    for (String name :
        new String[] {"", "*", ".", "..", "a/b", "a=b", "a/b=c", "a b", "a$b", "caf\u00e9"}) {
      CapabilityResult result = capability.specificationOnName(Scope.TOPIC, name);
      Assertions.assertFalse(result.supported(), name);
      Assertions.assertEquals(
          "Kafka topic names in Gravitino must be 1 to 128 characters long and contain only ASCII"
              + " alphanumerics, '_' and '-'.",
          result.unsupportedMessage());
    }
  }

  @Test
  void testGravitinoTopicNameRestrictions() {
    for (String name : new String[] {"a.b", "a".repeat(129)}) {
      CapabilityResult result = capability.specificationOnName(Scope.TOPIC, name);
      Assertions.assertFalse(result.supported(), name);
      Assertions.assertTrue(result.unsupportedMessage().contains("1 to 128 characters"));
    }
  }

  @Test
  void testOtherScopesUseDefaultNameSpecification() {
    for (Scope scope : Scope.values()) {
      if (scope == Scope.TOPIC) {
        continue;
      }
      for (String name :
          new String[] {"a/b", "a=b", "a.b", "*", "-topic", "a".repeat(65), "a".repeat(128)}) {
        CapabilityResult expected = Capability.DEFAULT.specificationOnName(scope, name);
        CapabilityResult actual = capability.specificationOnName(scope, name);
        Assertions.assertEquals(expected.supported(), actual.supported(), scope + ": " + name);
        Assertions.assertEquals(
            expected.unsupportedMessage(), actual.unsupportedMessage(), scope + ": " + name);
      }
    }
  }

  @Test
  void testCreateTopicRejectsIllegalNamesBeforeDispatch() throws IllegalAccessException {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    Mockito.when(catalogManager.doWithCatalog(Mockito.any(), Mockito.any())).thenReturn(capability);
    TopicDispatcher dispatcher = Mockito.mock(TopicDispatcher.class);
    TopicOperations operations =
        new TopicOperations(new TopicNormalizeDispatcher(dispatcher, catalogManager));
    FieldUtils.writeField(operations, "httpRequest", Mockito.mock(HttpServletRequest.class), true);

    Map<String, String> properties = Map.of("partition-count", "1", "replication-factor", "1");
    for (String name : new String[] {"a/b", "a=b", "a.b", "a".repeat(129)}) {
      TopicCreateRequest request = new TopicCreateRequest(name, null, properties);
      try (Response response = operations.createTopic("metalake", "kafka", "default", request)) {
        Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        ErrorResponse error = (ErrorResponse) response.getEntity();
        Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, error.getCode());
        Assertions.assertEquals(IllegalArgumentException.class.getSimpleName(), error.getType());
        Assertions.assertTrue(error.getMessage().contains(name));
        Assertions.assertTrue(
            error
                .getMessage()
                .contains(
                    "Kafka topic names in Gravitino must be 1 to 128 characters long and contain only"
                        + " ASCII alphanumerics, '_' and '-'."));
      }
    }
    Mockito.verifyNoInteractions(dispatcher);
  }
}
