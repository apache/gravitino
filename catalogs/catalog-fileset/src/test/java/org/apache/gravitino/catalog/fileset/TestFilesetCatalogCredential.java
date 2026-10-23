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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.credential.COSSecretKeyCredential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.COSProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies fileset catalog credential behavior, including hidden cloud credential properties (issue
 * #11642) and S3 location-prefix defaults.
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
  void testCatalogPropertiesMaskCloudCredentials() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "s3-ak")
            .put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "s3-sk")
            .put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, "oss-ak")
            .put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, "oss-sk")
            .put(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME, "abs-account")
            .put(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY, "abs-key")
            .put(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, "cos-ak")
            .put(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, "cos-sk")
            .build();
    FilesetCatalogImpl catalog = newCatalog(properties);

    Map<String, String> masked = catalog.properties();
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE, masked.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        masked.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        masked.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        masked.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET));
    Assertions.assertEquals(
        "abs-account", masked.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        masked.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        masked.get(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID));
    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE,
        masked.get(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET));
  }

  @Test
  void testCosCredentialProviderAutoDetected() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, "cos-ak");
    properties.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, "cos-sk");
    FilesetCatalogImpl catalog = newCatalog(properties);
    Assertions.assertTrue(
        catalog
            .propertiesWithCredentialProviders()
            .get(CredentialConstants.CREDENTIAL_PROVIDERS)
            .contains(COSSecretKeyCredential.COS_SECRET_KEY_CREDENTIAL_TYPE));
  }
}
