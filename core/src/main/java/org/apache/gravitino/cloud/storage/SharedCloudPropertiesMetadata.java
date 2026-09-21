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
package org.apache.gravitino.cloud.storage;

import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Shared credential {@link PropertyEntry} definitions merged into every catalog's properties
 * metadata.
 *
 * <p>Includes cloud-storage keys plus connector-specific credential keys (Glue AWS static keys,
 * JDBC password, Paimon REST token and DLF keys). Hidden flags match the connector that owns the
 * key. A connector that already declares the same key keeps its own entry.
 */
public final class SharedCloudPropertiesMetadata {

  /**
   * Connector-owned credential keys that are not part of the shared cloud-storage metadata maps.
   */
  private static final Map<String, PropertyEntry<?>> CONNECTOR_CREDENTIAL_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              "aws-access-key-id",
              stringOptionalPropertyEntry(
                  "aws-access-key-id",
                  "AWS access key ID for static credential authentication",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "aws-secret-access-key",
              stringOptionalPropertyEntry(
                  "aws-secret-access-key",
                  "AWS secret access key paired with aws-access-key-id",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "jdbc-password",
              stringOptionalPropertyEntry(
                  "jdbc-password",
                  "JDBC password",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "token",
              stringOptionalPropertyEntry(
                  "token",
                  "Bearer token for REST catalog authentication",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "dlf-access-key-id",
              stringOptionalPropertyEntry(
                  "dlf-access-key-id",
                  "Access key ID for Aliyun DLF",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              "dlf-access-key-secret",
              stringOptionalPropertyEntry(
                  "dlf-access-key-secret",
                  "Access key secret for Aliyun DLF",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              "dlf-security-token",
              stringOptionalPropertyEntry(
                  "dlf-security-token",
                  "Security token for Aliyun DLF",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .build();

  /** Cloud and connector credential keys merged into every catalog's properties metadata. */
  public static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .putAll(S3PropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(OSSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(AzurePropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(GCSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(COSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(CONNECTOR_CREDENTIAL_PROPERTY_ENTRIES)
          .build();

  private SharedCloudPropertiesMetadata() {}
}
