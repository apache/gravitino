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
package org.apache.gravitino.connector;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.COSTokenCredential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialUtils;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBaseCatalogCredentialProviders {

  @Test
  void testInferredCredentialProvidersExposed() throws IllegalAccessException {
    Map<String, String> props = new HashMap<>();
    props.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "access-key");
    props.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "secret-key");

    Map<String, String> result = newCatalog(props).properties();
    Assertions.assertEquals(
        S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE,
        result.get(CredentialConstants.CREDENTIAL_PROVIDERS));
  }

  @Test
  void testExplicitCredentialProvidersKept() throws IllegalAccessException {
    Map<String, String> props = new HashMap<>();
    props.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "access-key");
    props.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "secret-key");
    props.put(CredentialConstants.CREDENTIAL_PROVIDERS, "s3-token");

    Map<String, String> result = newCatalog(props).properties();
    Assertions.assertEquals("s3-token", result.get(CredentialConstants.CREDENTIAL_PROVIDERS));
  }

  @Test
  void testNoCredentialProvidersWithoutCredentials() throws IllegalAccessException {
    Map<String, String> props = new HashMap<>();
    props.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "access-key");

    Map<String, String> result = newCatalog(props).properties();
    Assertions.assertFalse(result.containsKey(CredentialConstants.CREDENTIAL_PROVIDERS));
  }

  @Test
  void testPathBasedCredentialTypes() {
    Assertions.assertEquals(
        ImmutableSet.of(
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE,
            AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE,
            OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE,
            GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE,
            ADLSTokenCredential.ADLS_TOKEN_CREDENTIAL_TYPE,
            COSTokenCredential.COS_TOKEN_CREDENTIAL_TYPE),
        CredentialUtils.PATH_BASED_CREDENTIAL_TYPES);
  }

  @Test
  void testValidateCredentialProviders() throws IllegalAccessException {
    TestCatalogImpl catalog = newCatalog(new NoPathBasedCredentialsCatalogImpl(), new HashMap<>());
    Assertions.assertDoesNotThrow(() -> catalog.validateCredentialProviders(null));
    Assertions.assertDoesNotThrow(
        () ->
            catalog.validateCredentialProviders(
                ImmutableMap.of(
                    CredentialConstants.CREDENTIAL_PROVIDERS,
                    "s3-secret-key,oss-secret-key,azure-account-key")));

    for (String type : CredentialUtils.PATH_BASED_CREDENTIAL_TYPES) {
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  catalog.validateCredentialProviders(
                      ImmutableMap.of(
                          CredentialConstants.CREDENTIAL_PROVIDERS, "s3-secret-key," + type)));
      Assertions.assertTrue(exception.getMessage().contains(type));
      Assertions.assertTrue(exception.getMessage().contains("test"));
    }

    // Provider lookup ignores case, so validation does too.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.validateCredentialProviders(
                ImmutableMap.of(CredentialConstants.CREDENTIAL_PROVIDERS, "GCS-Token")));
  }

  @Test
  void testGcsTokenInferredOnlyWithPathBasedCredentials() throws IllegalAccessException {
    Map<String, String> props = new HashMap<>();
    props.put(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, "/tmp/gcs.json");

    Map<String, String> result =
        newCatalog(new NoPathBasedCredentialsCatalogImpl(), props).properties();
    Assertions.assertFalse(result.containsKey(CredentialConstants.CREDENTIAL_PROVIDERS));

    Assertions.assertEquals(
        GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE,
        newCatalog(props).properties().get(CredentialConstants.CREDENTIAL_PROVIDERS));
  }

  @Test
  void testPathBasedCredentialsSupportedByDefault() throws IllegalAccessException {
    // A catalog that does not override supportsPathBasedCredentials() keeps accepting token
    // providers.
    TestCatalogImpl catalog = newCatalog(new HashMap<>());
    Assertions.assertTrue(catalog.supportsPathBasedCredentials());
    Assertions.assertDoesNotThrow(
        () ->
            catalog.validateCredentialProviders(
                ImmutableMap.of(CredentialConstants.CREDENTIAL_PROVIDERS, "s3-token,gcs-token")));
  }

  private TestCatalogImpl newCatalog(Map<String, String> props) throws IllegalAccessException {
    return newCatalog(new TestCatalogImpl(), props);
  }

  private TestCatalogImpl newCatalog(TestCatalogImpl catalog, Map<String, String> props)
      throws IllegalAccessException {
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();
    FieldUtils.writeField(catalog, "entity", entity, true);
    return catalog;
  }

  private static class TestCatalogImpl extends BaseCatalog<TestCatalogImpl> {

    @Override
    public String shortName() {
      return "test";
    }

    @Override
    protected CatalogOperations newOps(Map<String, String> config) {
      return Mockito.mock(CatalogOperations.class);
    }

    @Override
    protected Capability newCapability() {
      return Capability.DEFAULT;
    }

    @Override
    public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
      return BaseCatalogPropertiesMetadata.BASIC_CATALOG_PROPERTIES_METADATA;
    }
  }

  private static class NoPathBasedCredentialsCatalogImpl extends TestCatalogImpl {

    @Override
    public boolean supportsPathBasedCredentials() {
      return false;
    }
  }
}
