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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
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

  private TestCatalogImpl newCatalog(Map<String, String> props) throws IllegalAccessException {
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
    TestCatalogImpl catalog = new TestCatalogImpl();
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
}
