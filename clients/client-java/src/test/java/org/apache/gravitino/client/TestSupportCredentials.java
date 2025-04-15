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

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.credential.CredentialDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.file.Fileset;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSupportCredentials extends TestBase {

  private static final String METALAKE_NAME = "metalake";

  private static Catalog filesetCatalog;

  private static Fileset genericFileset;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    TestGravitinoMetalake.createMetalake(client, METALAKE_NAME);

    filesetCatalog =
        new FilesetCatalog(
            Namespace.of(METALAKE_NAME),
            "catalog2",
            Catalog.Type.FILESET,
            "test",
            "comment",
            Collections.emptyMap(),
            AuditDTO.builder().build(),
            client.restClient());

    genericFileset =
        new GenericFileset(
            FilesetDTO.builder()
                .name("fileset1")
                .comment("comment1")
                .type(Fileset.Type.EXTERNAL)
                .storageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "s3://bucket/path"))
                .properties(Collections.emptyMap())
                .audit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            Namespace.of(METALAKE_NAME, "catalog1", "schema1"));
  }

  @Test
  public void testGetCredentialsForCatalog() throws JsonProcessingException {
    testGetCredentials(
        filesetCatalog.supportsCredentials(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testGetCredentialsForFileset() throws JsonProcessingException {
    testGetCredentials(
        genericFileset.supportsCredentials(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  private void testGetCredentials(
      SupportsCredentials supportsCredentials, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/credentials";

    S3SecretKeyCredential secretKeyCredential =
        new S3SecretKeyCredential("access-id", "secret-key");
    GCSTokenCredential gcsTokenCredential = new GCSTokenCredential("token", 100);

    // Return one credential
    CredentialResponse resp =
        new CredentialResponse(DTOConverters.toDTO(new Credential[] {secretKeyCredential}));
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    Credential[] credentials = supportsCredentials.getCredentials();
    Assertions.assertEquals(1, credentials.length);

    Assertions.assertTrue(credentials[0] instanceof S3SecretKeyCredential);
    S3SecretKeyCredential s3CredentialInClient = (S3SecretKeyCredential) credentials[0];
    Assertions.assertEquals("access-id", s3CredentialInClient.accessKeyId());
    Assertions.assertEquals("secret-key", s3CredentialInClient.secretAccessKey());
    Assertions.assertEquals(0, s3CredentialInClient.expireTimeInMs());

    // Return multi credentials
    resp =
        new CredentialResponse(
            DTOConverters.toDTO(new Credential[] {secretKeyCredential, gcsTokenCredential}));
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    credentials = supportsCredentials.getCredentials();
    Assertions.assertEquals(2, credentials.length);

    Assertions.assertTrue(credentials[0] instanceof S3SecretKeyCredential);
    s3CredentialInClient = (S3SecretKeyCredential) credentials[0];
    Assertions.assertEquals("access-id", s3CredentialInClient.accessKeyId());
    Assertions.assertEquals("secret-key", s3CredentialInClient.secretAccessKey());
    Assertions.assertEquals(0, s3CredentialInClient.expireTimeInMs());

    Assertions.assertTrue(credentials[1] instanceof GCSTokenCredential);
    GCSTokenCredential gcsCredentialInClient = (GCSTokenCredential) credentials[1];
    Assertions.assertEquals("token", gcsCredentialInClient.token());
    Assertions.assertEquals(100, gcsCredentialInClient.expireTimeInMs());

    // Return empty list
    resp = new CredentialResponse(new CredentialDTO[0]);
    buildMockResource(Method.GET, path, null, resp, SC_OK);
    credentials = supportsCredentials.getCredentials();
    Assertions.assertEquals(0, credentials.length);

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, () -> supportsCredentials.getCredentials());
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }
}
