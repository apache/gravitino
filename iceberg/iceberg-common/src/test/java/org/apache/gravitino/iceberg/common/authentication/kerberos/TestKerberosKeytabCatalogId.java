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

package org.apache.gravitino.iceberg.common.authentication.kerberos;

import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKerberosKeytabCatalogId {

  @Test
  void testResolvePrefersCatalogUuid() {
    Map<String, String> properties =
        Map.of(
            KerberosKeytabCatalogId.CATALOG_UUID_KEY,
            "uuid-1",
            IcebergConstants.CATALOG_BACKEND_NAME,
            "catalog-a");

    Assertions.assertEquals("uuid-1", KerberosKeytabCatalogId.resolve(properties, "catalog-b"));
  }

  @Test
  void testResolveUsesCatalogBackendNameWhenUuidMissing() {
    Map<String, String> properties = Map.of(IcebergConstants.CATALOG_BACKEND_NAME, "catalog-a");

    Assertions.assertEquals("catalog-a", KerberosKeytabCatalogId.resolve(properties, "catalog-b"));
  }

  @Test
  void testResolveUsesCatalogNameWhenUuidAndBackendNameMissing() {
    Assertions.assertEquals("catalog-b", KerberosKeytabCatalogId.resolve(Map.of(), "catalog-b"));
  }

  @Test
  void testResolveFailsWhenAllIdentifiersMissing() {
    Assertions.assertThrows(
        IllegalStateException.class, () -> KerberosKeytabCatalogId.resolve(Map.of(), ""));
  }
}
