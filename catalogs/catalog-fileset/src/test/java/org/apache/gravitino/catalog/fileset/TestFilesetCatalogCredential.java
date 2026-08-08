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
package org.apache.gravitino.catalog.fileset;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies the fileset catalog credential behavior: the internal S3 location-prefix flag is enabled
 * regardless of any user-provided value, static storage credentials are hidden from the
 * outward-facing {@code properties()}, and they remain available for credential vending via {@code
 * propertiesWithCredentialProviders()}.
 */
public class TestFilesetCatalogCredential {

  private static FilesetCatalogImpl newCatalog(Map<String, String> properties) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.FILESET)
            .withProvider("fileset")
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();
    return new FilesetCatalogImpl().withCatalogConf(properties).withCatalogEntity(entity);
  }

  @Test
  void testLocationPrefixEnabledByDefault() {
    FilesetCatalogImpl catalog = newCatalog(Maps.newHashMap());
    Assertions.assertEquals(
        "true",
        catalog
            .propertiesWithCredentialProviders()
            .get(CredentialConstants.S3_CREDENTIAL_LIST_LOCATION_PREFIX));
  }

  @Test
  void testLocationPrefixNotUserConfigurable() {
    // A user attempt to disable it is overridden by the catalog type.
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CredentialConstants.S3_CREDENTIAL_LIST_LOCATION_PREFIX, "false");
    FilesetCatalogImpl catalog = newCatalog(properties);
    Assertions.assertEquals(
        "true",
        catalog
            .propertiesWithCredentialProviders()
            .get(CredentialConstants.S3_CREDENTIAL_LIST_LOCATION_PREFIX));
  }

  @Test
  void testStaticCredentialsHiddenFromCatalogProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "ak");
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "sk");
    properties.put(S3Properties.GRAVITINO_S3_ENDPOINT, "https://s3.example.com");
    FilesetCatalogImpl catalog = newCatalog(properties);

    Map<String, String> exposed = catalog.properties();
    // Sensitive credentials are filtered out from the outward-facing properties.
    Assertions.assertFalse(exposed.containsKey(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    Assertions.assertFalse(exposed.containsKey(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    // Non-sensitive connection info remains visible.
    Assertions.assertEquals(
        "https://s3.example.com", exposed.get(S3Properties.GRAVITINO_S3_ENDPOINT));
  }

  @Test
  void testStaticCredentialsStillAvailableForCredentialVending() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "ak");
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "sk");
    FilesetCatalogImpl catalog = newCatalog(properties);

    // Hiding the credentials does not break credential vending: the raw credentials are still
    // available to the server-side credential manager, and the matching credential provider is
    // auto-injected so clients (e.g. GVFS) can obtain vended credentials.
    Map<String, String> credProps = catalog.propertiesWithCredentialProviders();
    Assertions.assertEquals("ak", credProps.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    Assertions.assertEquals("sk", credProps.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    List<String> providers =
        Arrays.asList(credProps.get(CredentialConstants.CREDENTIAL_PROVIDERS).split(","));
    Assertions.assertTrue(providers.contains(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE));
  }
}
