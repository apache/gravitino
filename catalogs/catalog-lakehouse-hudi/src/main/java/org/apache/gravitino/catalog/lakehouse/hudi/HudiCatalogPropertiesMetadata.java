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
package org.apache.gravitino.catalog.lakehouse.hudi;

import static org.apache.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.hudi.backend.BackendType;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.hive.ClientPropertiesMetadata;

public class HudiCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  public static final String CATALOG_BACKEND = "catalog-backend";
  public static final String URI = "uri";
  private static final ClientPropertiesMetadata CLIENT_PROPERTIES_METADATA =
      new ClientPropertiesMetadata();

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              CATALOG_BACKEND,
              enumImmutablePropertyEntry(
                  CATALOG_BACKEND,
                  "Hudi catalog type choose properties",
                  true /* required */,
                  BackendType.class,
                  null /* defaultValue */,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              URI,
              stringRequiredPropertyEntry(
                  URI, "Hudi catalog uri config", false /* immutable */, false /* hidden */))
          .putAll(CLIENT_PROPERTIES_METADATA.propertyEntries())
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
