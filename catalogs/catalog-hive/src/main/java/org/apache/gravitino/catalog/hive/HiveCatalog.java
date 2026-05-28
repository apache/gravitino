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
package org.apache.gravitino.catalog.hive;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

/** Implementation of an Apache Hive catalog in Apache Gravitino. */
public class HiveCatalog extends BaseCatalog<HiveCatalog> {

  static final HiveCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new HiveCatalogPropertiesMetadata();

  static final HiveSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new HiveSchemaPropertiesMetadata();

  static final HiveTablePropertiesMetadata TABLE_PROPERTIES_METADATA =
      new HiveTablePropertiesMetadata();

  /**
   * Returns the short name of the Hive catalog.
   *
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "hive";
  }

  /**
   * Creates a new instance of {@link HiveCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Hive catalog operations.
   * @return A new instance of {@link HiveCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    HiveCatalogOperations ops = new HiveCatalogOperations();
    return ops;
  }

  @Override
  public Capability newCapability() {
    return new HiveCatalogCapability();
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_METADATA;
  }

  @Override
  @Evolving
  public Map<String, String> propertiesWithCredentialProviders() {
    // Use raw entity properties so hidden credentials are visible to the credential manager.
    Map<String, String> properties = Maps.newHashMap(entity().getProperties());
    if (StringUtils.isNotBlank(properties.get(CredentialConstants.CREDENTIAL_PROVIDERS))) {
      return properties;
    }
    List<String> credentialProviders = new ArrayList<>();
    addStorageCredentialProviders(properties, credentialProviders);
    if (!credentialProviders.isEmpty()) {
      properties.put(
          CredentialConstants.CREDENTIAL_PROVIDERS, String.join(",", credentialProviders));
    }
    return properties;
  }

  /**
   * Returns catalog properties, optionally re-adding hidden credentials for backward compatibility
   * with connectors that do not support credential vending. Controlled by server-level config
   * {@code gravitino.catalog.credential.backfillToProperties}.
   *
   * @return the catalog properties map, with credentials backfilled if the server config is set
   */
  @Override
  public Map<String, String> properties() {
    Map<String, String> props = super.properties();
    if (!shouldBackfillCredential()) {
      return props;
    }
    Map<String, String> rawProps = entity().getProperties();
    Map<String, String> result = Maps.newHashMap(props);
    backfillIfPresent(rawProps, result, S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
    backfillIfPresent(rawProps, result, OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET);
    backfillIfPresent(rawProps, result, AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    return result;
  }
}
