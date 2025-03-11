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

package org.apache.gravitino.lakehouse.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergPropertiesUtils {

  @Test
  void testGetCatalogBackendName() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND_NAME, "a", IcebergConstants.CATALOG_BACKEND, "jdbc");
    String backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("a", backendName);

    catalogProperties = ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "jdbc");
    backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("jdbc", backendName);

    catalogProperties = ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "JDBC");
    backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("jdbc", backendName);

    catalogProperties = ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "hive");
    backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("hive", backendName);

    catalogProperties = ImmutableMap.of();
    backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("memory", backendName);
  }
}
