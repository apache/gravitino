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
package org.apache.gravitino.catalog.lakehouse.hudi.utils;

import static org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata.CATALOG_BACKEND;

import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.hudi.backend.BackendType;
import org.apache.gravitino.catalog.lakehouse.hudi.backend.HudiCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.hudi.backend.hms.HudiHMSBackend;

public class CatalogUtils {
  private CatalogUtils() {}

  public static HudiCatalogBackend loadHudiCatalogBackend(Map<String, String> properties) {
    BackendType backendType =
        BackendType.valueOf(properties.get(CATALOG_BACKEND).toUpperCase(Locale.ROOT));
    switch (backendType) {
      case HMS:
        HudiCatalogBackend hudiHMSBackend = new HudiHMSBackend();
        hudiHMSBackend.initialize(properties);
        return hudiHMSBackend;
      default:
        throw new UnsupportedOperationException("Unsupported backend type: " + backendType);
    }
  }
}
