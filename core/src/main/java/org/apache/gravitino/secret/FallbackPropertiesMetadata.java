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
package org.apache.gravitino.secret;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.cloud.storage.AWSPropertiesMetadata;
import org.apache.gravitino.cloud.storage.AzurePropertiesMetadata;
import org.apache.gravitino.cloud.storage.COSPropertiesMetadata;
import org.apache.gravitino.cloud.storage.GCSPropertiesMetadata;
import org.apache.gravitino.cloud.storage.OSSPropertiesMetadata;
import org.apache.gravitino.cloud.storage.S3PropertiesMetadata;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Fallback {@link org.apache.gravitino.connector.PropertiesMetadata} when a catalog does not
 * support properties metadata for an entity type ({@link UnsupportedOperationException}).
 *
 * <p>Registers the shared base + credential-vending + cloud-storage property entries so officially
 * non-hidden keys (for example {@code credential-providers}, {@code s3-access-key-id}) are not
 * fuzzy-recovered into {@code getSecrets}. Undeclared sensitive-named keys still use fuzzy
 * recovery; declared hidden secrets (for example {@code s3-secret-access-key}) remain recoverable.
 */
final class FallbackPropertiesMetadata extends BasePropertiesMetadata {

  static final FallbackPropertiesMetadata INSTANCE = new FallbackPropertiesMetadata();

  private static final Map<String, PropertyEntry<?>> CLOUD_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .putAll(S3PropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(OSSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(AzurePropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(GCSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(COSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(AWSPropertiesMetadata.PROPERTY_ENTRIES)
          .build();

  private FallbackPropertiesMetadata() {}

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return CLOUD_PROPERTY_ENTRIES;
  }
}
