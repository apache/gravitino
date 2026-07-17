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

package org.apache.gravitino.filesystem.hadoop.integration.test;

import static org.apache.gravitino.catalog.fileset.FilesetCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.filesystem.hadoop.DefaultGravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.s3.credential.webidentity.WebIdentityTokenSourceConfig;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FilesetS3WebIdentityCredentialIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(FilesetS3WebIdentityCredentialIT.class);
  private static final String WEB_IDENTITY_TOKEN = "configured-web-identity-token";

  @TempDir private static Path tempDir;

  private final AtomicReference<String> requestBody = new AtomicReference<>();

  private HttpServer stsServer;
  private Path webIdentityTokenFile;
  private String metalakeName;
  private String catalogName;
  private String schemaName;
  private String filesetName;
  private GravitinoMetalake metalake;

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing.
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("aws-bundle");
    stsServer = startFakeStsServer();
    webIdentityTokenFile = tempDir.resolve("web-identity-token");
    Files.write(webIdentityTokenFile, WEB_IDENTITY_TOKEN.getBytes(StandardCharsets.UTF_8));

    super.startIntegrationTest();

    metalakeName = GravitinoITUtils.genRandomName("web_identity_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");
    filesetName = GravitinoITUtils.genRandomName("fileset");

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put(FILESYSTEM_PROVIDERS, "s3");
    properties.put("disable-filesystem-ops", "true");
    properties.put(CredentialConstants.CREDENTIAL_CACHE_EXPIRE_RATIO, "0");
    properties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE);
    properties.put(S3Properties.GRAVITINO_S3_ROLE_ARN, "arn:aws:iam::123456789012:role/test-role");
    properties.put(S3Properties.GRAVITINO_S3_REGION, "us-east-1");
    properties.put(S3Properties.GRAVITINO_S3_STS_ENDPOINT, fakeStsEndpoint());
    properties.put(WebIdentityTokenSourceConfig.SOURCE, "file");
    properties.put(WebIdentityTokenSourceConfig.FILE_PATH, webIdentityTokenFile.toString());

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    catalog.asSchemas().createSchema(schemaName, "schema comment", properties);
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));
    catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(schemaName, filesetName),
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation(),
            ImmutableMap.of());
  }

  @AfterAll
  public void tearDown() throws IOException {
    try {
      if (metalake != null && catalogName != null) {
        Catalog catalog = metalake.loadCatalog(catalogName);
        if (schemaName != null) {
          catalog.asSchemas().dropSchema(schemaName, true);
        }
        metalake.dropCatalog(catalogName, true);
      }
      if (client != null && metalakeName != null) {
        client.dropMetalake(metalakeName, true);
      }
    } finally {
      if (client != null) {
        client.close();
        client = null;
      }
      if (stsServer != null) {
        stsServer.stop(0);
      }
      try {
        closer.close();
      } catch (Exception e) {
        LOG.error("Exception in closing CloseableGroup", e);
      }
    }
  }

  @Test
  void testFilesetCredentialUsesFileWebIdentityTokenSource() {
    requestBody.set(null);
    Fileset fileset =
        metalake
            .loadCatalog(catalogName)
            .asFilesetCatalog()
            .loadFileset(NameIdentifier.of(schemaName, filesetName));

    Credential[] credentials = fileset.supportsCredentials().getCredentials();

    Assertions.assertEquals(1, credentials.length);
    Assertions.assertInstanceOf(AwsIrsaCredential.class, credentials[0]);
    assertFakeStsReceivedWebIdentityToken();
    Assertions.assertTrue(decodedRequestBody().contains("Policy="));
  }

  @Test
  void testGvfsCredentialProviderUsesFileWebIdentityTokenSource() {
    requestBody.set(null);
    DefaultGravitinoFileSystemCredentialsProvider provider =
        new DefaultGravitinoFileSystemCredentialsProvider();
    Configuration configuration = new Configuration();
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY, serverUri);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, metalakeName);
    configuration.set(
        GravitinoFileSystemCredentialsProvider.GVFS_NAME_IDENTIFIER,
        NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName).toString());
    provider.setConf(configuration);

    Credential[] credentials = provider.getCredentials();

    Assertions.assertEquals(1, credentials.length);
    Assertions.assertInstanceOf(AwsIrsaCredential.class, credentials[0]);
    assertFakeStsReceivedWebIdentityToken();
    Assertions.assertTrue(decodedRequestBody().contains("Policy="));
  }

  private HttpServer startFakeStsServer() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handleAssumeRoleWithWebIdentity);
    server.start();
    return server;
  }

  private void handleAssumeRoleWithWebIdentity(HttpExchange exchange) throws IOException {
    requestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
    byte[] response = assumeRoleWithWebIdentityResponse().getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/xml");
    exchange.sendResponseHeaders(200, response.length);
    exchange.getResponseBody().write(response);
    exchange.close();
  }

  private String fakeStsEndpoint() {
    return "http://127.0.0.1:" + stsServer.getAddress().getPort();
  }

  private String storageLocation() {
    return "s3a://bucket1/" + filesetName;
  }

  private void assertFakeStsReceivedWebIdentityToken() {
    String decodedBody = decodedRequestBody();
    Assertions.assertTrue(decodedBody.contains("Action=AssumeRoleWithWebIdentity"));
    Assertions.assertTrue(decodedBody.contains("WebIdentityToken=" + WEB_IDENTITY_TOKEN));
  }

  private String decodedRequestBody() {
    Assertions.assertNotNull(requestBody.get(), "Fake STS server did not receive a request");
    return URLDecoder.decode(requestBody.get(), StandardCharsets.UTF_8);
  }

  private String assumeRoleWithWebIdentityResponse() {
    return "<AssumeRoleWithWebIdentityResponse "
        + "xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
        + "<AssumeRoleWithWebIdentityResult>"
        + "<Credentials>"
        + "<AccessKeyId>test-access-key</AccessKeyId>"
        + "<SecretAccessKey>test-secret-key</SecretAccessKey>"
        + "<SessionToken>test-session-token</SessionToken>"
        + "<Expiration>2035-01-01T00:00:00Z</Expiration>"
        + "</Credentials>"
        + "<SubjectFromWebIdentityToken>subject</SubjectFromWebIdentityToken>"
        + "<Audience>audience</Audience>"
        + "<AssumedRoleUser>"
        + "<Arn>arn:aws:sts::123456789012:assumed-role/test-role/session</Arn>"
        + "<AssumedRoleId>test-role-id:test-session</AssumedRoleId>"
        + "</AssumedRoleUser>"
        + "</AssumeRoleWithWebIdentityResult>"
        + "<ResponseMetadata><RequestId>test-request</RequestId></ResponseMetadata>"
        + "</AssumeRoleWithWebIdentityResponse>";
  }
}
