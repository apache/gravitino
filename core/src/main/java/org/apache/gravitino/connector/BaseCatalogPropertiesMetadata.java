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

import static org.apache.gravitino.Catalog.CLOUD_NAME;
import static org.apache.gravitino.Catalog.CLOUD_REGION_CODE;
import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.Catalog.PROPERTY_PACKAGE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.annotation.Evolving;

@Evolving
public abstract class BaseCatalogPropertiesMetadata extends BasePropertiesMetadata {

  public static final PropertiesMetadata BASIC_CATALOG_PROPERTIES_METADATA =
      new BaseCatalogPropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return Collections.emptyMap();
        }
      };

  // The basic property entries for catalog entities
  private static final Map<String, PropertyEntry<?>> BASIC_CATALOG_PROPERTY_ENTRIES =
      Maps.uniqueIndex(
          ImmutableList.of(
              PropertyEntry.stringImmutablePropertyEntry(
                  PROPERTY_PACKAGE,
                  "The path of the catalog-related classes and resources",
                  false,
                  null,
                  false,
                  false),
              PropertyEntry.stringImmutablePropertyEntry(
                  BaseCatalog.CATALOG_OPERATION_IMPL,
                  "The classname of custom catalog operation to replace the default implementation",
                  false,
                  null,
                  false,
                  false),
              PropertyEntry.stringImmutablePropertyEntry(
                  Catalog.AUTHORIZATION_PROVIDER,
                  "The name of the authorization provider for Gravitino",
                  false,
                  null,
                  false,
                  false),
              PropertyEntry.enumPropertyEntry(
                  CLOUD_NAME,
                  "The cloud that the catalog is running on",
                  false /* required */,
                  false /* immutable */,
                  Catalog.CloudName.class,
                  null /* The default value does not work because if the user does not set it, this property will not be displayed */,
                  false /* hidden */,
                  false /* reserved */),
              PropertyEntry.stringOptionalPropertyEntry(
                  CLOUD_REGION_CODE,
                  "The region code of the cloud that the catalog is running on",
                  false /* immutable */,
                  null /* The default value does not work because if the user does not set it, this property will not be displayed */,
                  false /* hidden */),
              PropertyEntry.booleanReservedPropertyEntry(
                  PROPERTY_IN_USE,
                  "The property indicating the catalog is in use",
                  true /* default value */,
                  false /* hidden */)),
          PropertyEntry::getName);

  @Override
  public Map<String, PropertyEntry<?>> propertyEntries() {
    if (propertyEntries == null) {
      synchronized (this) {
        if (propertyEntries == null) {
          ImmutableMap.Builder<String, PropertyEntry<?>> builder = ImmutableMap.builder();
          Map<String, PropertyEntry<?>> properties = specificPropertyEntries();
          builder.putAll(properties);

          // put the basic property entries
          BASIC_PROPERTY_ENTRIES.forEach(
              (name, entry) -> {
                Preconditions.checkArgument(
                    !properties.containsKey(name), "Property metadata already exists: " + name);
                builder.put(name, entry);
              });

          // put the basic catalog property entries
          BASIC_CATALOG_PROPERTY_ENTRIES.forEach(
              (name, entry) -> {
                Preconditions.checkArgument(
                    !properties.containsKey(name), "Property metadata already exists: " + name);
                builder.put(name, entry);
              });
          propertyEntries = builder.build();
        }
      }
    }
    return propertyEntries;
  }
}
