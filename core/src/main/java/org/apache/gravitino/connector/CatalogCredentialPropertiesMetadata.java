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

import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;

/**
 * Connector credential {@link PropertyEntry} definitions.
 *
 * <p>Hidden is defined only here. Glue and Paimon metadata reference these entries instead of
 * setting hidden again. {@link org.apache.gravitino.cloud.storage.SharedCloudPropertiesMetadata}
 * merges this map into catalogs that do not already declare the key. JDBC user and password stay on
 * the JDBC, Iceberg, and Paimon catalogs, because they are not cloud credentials.
 */
public final class CatalogCredentialPropertiesMetadata {

  /** AWS access key ID. Not hidden. */
  public static final PropertyEntry<String> AWS_ACCESS_KEY_ID =
      stringOptionalPropertyEntry(
          GlueConstants.AWS_ACCESS_KEY_ID,
          "AWS access key ID for static credential authentication."
              + " When omitted the default credential chain is used.",
          false /* immutable */,
          null /* defaultValue */,
          false /* hidden */);

  /** AWS secret access key. Hidden. */
  public static final PropertyEntry<String> AWS_SECRET_ACCESS_KEY =
      stringOptionalPropertyEntry(
          GlueConstants.AWS_SECRET_ACCESS_KEY,
          "AWS secret access key paired with aws-access-key-id."
              + " When omitted the default credential chain is used.",
          false /* immutable */,
          null /* defaultValue */,
          true /* hidden */);

  /** Paimon REST and DLF credential keys, including non-secret companions. */
  public static final Map<String, PropertyEntry<?>> PAIMON_REST_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              PaimonConstants.GRAVITINO_TOKEN_PROVIDER,
              stringOptionalPropertyEntry(
                  PaimonConstants.GRAVITINO_TOKEN_PROVIDER,
                  "The token provider type for Paimon",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              PaimonConstants.TOKEN,
              stringOptionalPropertyEntry(
                  PaimonConstants.TOKEN,
                  "The bearer token for REST catalog authentication",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID,
              stringOptionalPropertyEntry(
                  PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID,
                  "The access key ID for Aliyun DLF",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET,
              stringOptionalPropertyEntry(
                  PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET,
                  "The access key secret for Aliyun DLF",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN,
              stringOptionalPropertyEntry(
                  PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN,
                  "The security token for Aliyun DLF",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              PaimonConstants.GRAVITINO_DLF_TOKEN_PATH,
              stringOptionalPropertyEntry(
                  PaimonConstants.GRAVITINO_DLF_TOKEN_PATH,
                  "The token path for Aliyun DLF",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              PaimonConstants.GRAVITINO_DLF_TOKEN_LOADER,
              stringOptionalPropertyEntry(
                  PaimonConstants.GRAVITINO_DLF_TOKEN_LOADER,
                  "The token loader for Aliyun DLF",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .build();

  /** Glue and Paimon credential keys. */
  public static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(AWS_ACCESS_KEY_ID.getName(), AWS_ACCESS_KEY_ID)
          .put(AWS_SECRET_ACCESS_KEY.getName(), AWS_SECRET_ACCESS_KEY)
          .putAll(PAIMON_REST_PROPERTY_ENTRIES)
          .build();

  private CatalogCredentialPropertiesMetadata() {}
}
