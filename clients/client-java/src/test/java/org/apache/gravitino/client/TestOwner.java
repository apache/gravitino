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

import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.dto.requests.OwnerSetRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.responses.OwnerResponse;
import org.apache.gravitino.dto.responses.SetResponse;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestOwner extends TestBase {

  private static final String metalakeName = "testMetalake";
  private static final String API_OWNER_PATH = "api/metalakes/%s/owners/%s";
  private static GravitinoClient gravitinoClient;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    TestGravitinoMetalake.createMetalake(client, metalakeName);

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

  @Test
  public void testGetOwner() throws JsonProcessingException {
    MetadataObject object = MetadataObjects.of(null, metalakeName, MetadataObject.Type.METALAKE);
    String ownerPath =
        withSlash(
            String.format(
                API_OWNER_PATH,
                metalakeName,
                String.format(
                    "%s/%s", object.type().name().toLowerCase(Locale.ROOT), object.fullName())));
    OwnerDTO ownerDTO = OwnerDTO.builder().withName("test").withType(Owner.Type.USER).build();
    OwnerResponse resp = new OwnerResponse(ownerDTO);
    buildMockResource(Method.GET, ownerPath, null, resp, SC_OK);
    Optional<Owner> owner = gravitinoClient.getOwner(object);
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals("test", owner.get().name());
    Assertions.assertEquals(Owner.Type.USER, owner.get().type());

    // no owner case
    OwnerResponse noOwnerResp = new OwnerResponse(null);
    buildMockResource(Method.GET, ownerPath, null, noOwnerResp, SC_OK);
    Assertions.assertFalse(gravitinoClient.getOwner(object).isPresent());

    // no such metadata object exception
    ErrorResponse errorResp1 =
        ErrorResponse.notFound(NoSuchMetadataObjectException.class.getSimpleName(), "not found");
    buildMockResource(Method.GET, ownerPath, null, errorResp1, SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(
            NoSuchMetadataObjectException.class, () -> gravitinoClient.getOwner(object));
    Assertions.assertTrue(ex1.getMessage().contains("not found"));
  }

  @Test
  public void testSetOwner() throws JsonProcessingException {
    MetadataObject object = MetadataObjects.of(null, metalakeName, MetadataObject.Type.METALAKE);
    String ownerPath =
        withSlash(
            String.format(
                API_OWNER_PATH,
                metalakeName,
                String.format(
                    "%s/%s", object.type().name().toLowerCase(Locale.ROOT), object.fullName())));
    OwnerSetRequest request = new OwnerSetRequest("test", Owner.Type.USER);
    SetResponse response = new SetResponse(true);
    buildMockResource(Method.PUT, ownerPath, request, response, SC_OK);

    Assertions.assertDoesNotThrow(() -> gravitinoClient.setOwner(object, "test", Owner.Type.USER));

    // not found exception
    ErrorResponse errorResp1 =
        ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "not found");
    buildMockResource(Method.PUT, ownerPath, request, errorResp1, SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(
            NotFoundException.class,
            () -> gravitinoClient.setOwner(object, "test", Owner.Type.USER));
    Assertions.assertTrue(ex1.getMessage().contains("not found"));
  }
}
