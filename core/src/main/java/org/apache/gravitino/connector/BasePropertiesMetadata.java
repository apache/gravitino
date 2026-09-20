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

import static org.apache.gravitino.StringIdentifier.ID_KEY;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.credential.config.CredentialConfig;

/**
 * An abstract class representing a base properties metadata for entities. Developers should extend
 * this class to implement a custom properties metadata for their entities, like table, fileset,
 * schema.
 *
 * <p>Note: For catalog related properties metadata, use {@link BaseCatalogPropertiesMetadata}.
 *
 * <p>This class defines reserved properties metadata for Gravitino use only. Developers should not
 * override these properties.
 */
@Evolving
public abstract class BasePropertiesMetadata implements PropertiesMetadata {

  protected static final Map<String, PropertyEntry<?>> BASIC_PROPERTY_ENTRIES;

  protected volatile Map<String, PropertyEntry<?>> propertyEntries;

  static {
    // basicPropertyEntries is shared by all entities
    List<PropertyEntry<?>> basicPropertyEntries =
        ImmutableList.of(
            PropertyEntry.stringReservedPropertyEntry(
                ID_KEY,
                "To differentiate the entities created directly by the underlying sources",
                true));

    BASIC_PROPERTY_ENTRIES = Maps.uniqueIndex(basicPropertyEntries, PropertyEntry::getName);
  }

  @Override
  public Map<String, PropertyEntry<?>> propertyEntries() {
    if (propertyEntries == null) {
      synchronized (this) {
        if (propertyEntries == null) {
          propertyEntries = buildBasePropertyEntries();
        }
      }
    }
    return propertyEntries;
  }

  /**
   * Builds specific + shared base property entries (including {@link CredentialConfig}) without
   * caching. Catalog metadata subclasses add catalog-only entries on top of this map.
   *
   * @return an immutable property entry map
   */
  protected final Map<String, PropertyEntry<?>> buildBasePropertyEntries() {
    ImmutableMap.Builder<String, PropertyEntry<?>> builder = ImmutableMap.builder();
    Map<String, PropertyEntry<?>> properties = specificPropertyEntries();
    checkConnectorSpecificPropertiesRegistered(properties);
    builder.putAll(properties);

    BASIC_PROPERTY_ENTRIES.forEach(
        (name, entry) -> {
          Preconditions.checkArgument(
              !properties.containsKey(name), "Property metadata already exists: " + name);
          builder.put(name, entry);
        });

    // Credential vending keys (e.g. credential-providers) are valid on schema / fileset / table as
    // well as catalog. Register once so catalog metadata includes the official entries; cross-
    // catalog fuzzy-mask consistency also uses RegisteredPropertyKeys.
    CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES.forEach(
        (name, entry) -> {
          Preconditions.checkArgument(
              !properties.containsKey(name), "Property metadata already exists: " + name);
          builder.put(name, entry);
        });

    return builder.build();
  }

  /**
   * Ensures every connector-specific property is registered in at least one of: shared base ({@link
   * #BASIC_PROPERTY_ENTRIES}), credential/cloud metadata ({@link
   * RegisteredPropertyKeys#isSharedCloudOrCredentialKey}), or {@link RegisteredPropertyKeys}
   * connector keys. Does not validate user-supplied entity property maps.
   */
  private void checkConnectorSpecificPropertiesRegistered(
      Map<String, PropertyEntry<?>> specificEntries) {
    if (specificEntries == null || specificEntries.isEmpty()) {
      return;
    }
    for (String name : specificEntries.keySet()) {
      if (BASIC_PROPERTY_ENTRIES.containsKey(name)
          || RegisteredPropertyKeys.isSharedCloudOrCredentialKey(name)
          || RegisteredPropertyKeys.isRegistered(name)) {
        continue;
      }
      Preconditions.checkArgument(
          false,
          "Connector-defined property '%s' in %s must be registered in base properties,"
              + " shared cloud/credential metadata, or RegisteredPropertyKeys."
              + " User entity custom properties are not blocked by this check.",
          name,
          getClass().getName());
    }
  }

  /**
   * Returns the specific property entries for the entity. Developers should override this method to
   * provide the specific property entries for their entities.
   *
   * @return The specific property entries for the entity.
   */
  @Evolving
  protected abstract Map<String, PropertyEntry<?>> specificPropertyEntries();
}
