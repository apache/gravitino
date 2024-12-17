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
package org.apache.gravitino.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.catalog.CredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchCredentialException;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetadataObjectCredentialOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private CredentialManager credentialManager = mock(CredentialManager.class);

  private String metalake = "test_metalake";

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(MetadataObjectCredentialOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(credentialManager).to(CredentialManager.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testGetCredentialsForCatalog() {
    testGetCredentialsForObject(MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG));
  }

  @Test
  public void testGetCredentialsForFileset() {
    testGetCredentialsForObject(
        MetadataObjects.parse("catalog.schema.fileset", MetadataObject.Type.FILESET));
  }

  private void testGetCredentialsForObject(MetadataObject metadataObject) {

    S3SecretKeyCredential credential = new S3SecretKeyCredential("access-id", "secret-key");
    // Test return one credential
    when(credentialManager.getCredentials(any())).thenReturn(Arrays.asList(credential));
    Response response =
        target(basePath(metalake))
            .path(metadataObject.type().toString())
            .path(metadataObject.fullName())
            .path("/credentials")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    CredentialResponse credentialResponse = response.readEntity(CredentialResponse.class);
    Assertions.assertEquals(0, credentialResponse.getCode());
    Assertions.assertEquals(1, credentialResponse.getCredentials().length);
    Credential credentialToTest = DTOConverters.fromDTO(credentialResponse.getCredentials()[0]);
    Assertions.assertTrue(credentialToTest instanceof S3SecretKeyCredential);
    Assertions.assertEquals("access-id", ((S3SecretKeyCredential) credentialToTest).accessKeyId());
    Assertions.assertEquals(
        "secret-key", ((S3SecretKeyCredential) credentialToTest).secretAccessKey());
    Assertions.assertEquals(0, credentialToTest.expireTimeInMs());

    // Test doesn't return credential
    when(credentialManager.getCredentials(any())).thenReturn(null);
    response =
        target(basePath(metalake))
            .path(metadataObject.type().toString())
            .path(metadataObject.fullName())
            .path("/credentials")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    credentialResponse = response.readEntity(CredentialResponse.class);
    Assertions.assertEquals(0, credentialResponse.getCode());
    Assertions.assertEquals(0, credentialResponse.getCredentials().length);

    // Test throws NoSuchCredentialException
    doThrow(new NoSuchCredentialException("mock error"))
        .when(credentialManager)
        .getCredentials(any());
    response =
        target(basePath(metalake))
            .path(metadataObject.type().toString())
            .path(metadataObject.fullName())
            .path("/credentials")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        NoSuchCredentialException.class.getSimpleName(), errorResponse.getType());
  }

  private String basePath(String metalake) {
    return "/metalakes/" + metalake + "/objects";
  }
}
