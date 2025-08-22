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
package org.apache.gravitino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.dto.policy.PolicyDTO;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetadataObjectListResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.policy.Policy;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestGenericPolicy extends TestBase {

  private static final String metalakeName = "metalake1";

  private static final PolicyDTO policyDTO =
      PolicyDTO.builder()
          .withName("policy1")
          .withComment("comment1")
          .withPolicyType("my_compaction")
          .withEnabled(true)
          .withContent(
              PolicyContentDTO.CustomContentDTO.builder()
                  .withCustomRules(ImmutableMap.of("key1", "value1"))
                  .withSupportedObjectTypes(ImmutableSet.of(MetadataObject.Type.TABLE))
                  .withProperties(ImmutableMap.of("prop1", "value1"))
                  .build())
          .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .build();

  private static GravitinoClient gravitinoClient;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName(metalakeName)
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.GET, "/api/metalakes/" + metalakeName, null, resp, HttpStatus.SC_OK);

    gravitinoClient =
        GravitinoClient.builder("http://127.0.0.1:" + mockServer.getLocalPort())
            .withMetalake(metalakeName)
            .withVersionCheckDisabled()
            .build();
  }

  @AfterAll
  public static void tearDown() {
    TestBase.tearDown();
    gravitinoClient.close();
  }

  @Test
  public void testAssociatedObjects() throws JsonProcessingException {
    Policy policy = new GenericPolicy(policyDTO, gravitinoClient.restClient(), metalakeName);
    String path = "/api/metalakes/" + metalakeName + "/policies/" + policyDTO.name() + "/objects";

    MetadataObjectDTO[] objects =
        new MetadataObjectDTO[] {
          MetadataObjectDTO.builder()
              .withParent(null)
              .withName("catalog1")
              .withType(MetadataObject.Type.CATALOG)
              .build(),
          MetadataObjectDTO.builder()
              .withParent("catalog1")
              .withName("schema1")
              .withType(MetadataObject.Type.SCHEMA)
              .build(),
          MetadataObjectDTO.builder()
              .withParent("catalog1.schema1")
              .withName("table1")
              .withType(MetadataObject.Type.TABLE)
              .build(),
        };

    MetadataObjectListResponse resp = new MetadataObjectListResponse(objects);
    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);

    MetadataObject[] actualObjects = policy.associatedObjects().objects();
    Assertions.assertEquals(objects.length, actualObjects.length);
    for (int i = 0; i < objects.length; i++) {
      MetadataObjectDTO object = objects[i];
      MetadataObject actualObject = actualObjects[i];
      Assertions.assertEquals(object.parent(), actualObject.parent());
      Assertions.assertEquals(object.name(), actualObject.name());
      Assertions.assertEquals(object.type(), actualObject.type());
    }

    // Test return empty array
    buildMockResource(
        Method.GET,
        path,
        null,
        new MetadataObjectListResponse(new MetadataObjectDTO[0]),
        HttpStatus.SC_OK);
    MetadataObject[] actualObjects1 = policy.associatedObjects().objects();
    Assertions.assertEquals(0, actualObjects1.length);

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);

    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> policy.associatedObjects().objects());
    Assertions.assertEquals("mock error", ex.getMessage());

    // Test throw internal error
    ErrorResponse errorResponse1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResponse1, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, () -> policy.associatedObjects().objects());
    Assertions.assertEquals("mock error", ex1.getMessage());
  }
}
