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
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the fileset catalog enables the internal S3 location-prefix flag so a
 * directory-root {@code getFileStatus} HEAD returns 404 instead of 403, regardless of any
 * user-provided value.
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
}
