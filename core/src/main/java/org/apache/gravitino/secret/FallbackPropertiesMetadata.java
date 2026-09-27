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

import java.util.Map;
import org.apache.gravitino.cloud.storage.CloudPropertiesMetadata;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Fallback {@link org.apache.gravitino.connector.PropertiesMetadata} when a catalog does not
 * support properties metadata for an entity type ({@link UnsupportedOperationException}).
 *
 * <p>Registers shared base + credential-vending + cloud-storage entries ({@link
 * CloudPropertiesMetadata#STORAGE_PROPERTY_ENTRIES}) so officially non-hidden keys (for example
 * {@code credential-providers}, {@code s3-access-key-id}) are not fuzzy-recovered into {@code
 * getSecrets}. Undeclared sensitive-named keys still use fuzzy recovery; declared hidden secrets
 * (for example {@code s3-secret-access-key}) remain recoverable.
 *
 * <p>Does not include the AWS access-key pair. That pair is a Glue catalog property merged only via
 * {@link org.apache.gravitino.connector.BaseCatalogPropertiesMetadata}, matching fileset and schema
 * metadata which also omit it. A catalog path that hits this fallback therefore treats {@code
 * aws-access-key-id} as undeclared (fuzzy mask / recover), the same as a fileset or schema.
 */
final class FallbackPropertiesMetadata extends BasePropertiesMetadata {

  static final FallbackPropertiesMetadata INSTANCE = new FallbackPropertiesMetadata();

  private static final Map<String, PropertyEntry<?>> CLOUD_PROPERTY_ENTRIES =
      CloudPropertiesMetadata.STORAGE_PROPERTY_ENTRIES;

  private FallbackPropertiesMetadata() {}

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return CLOUD_PROPERTY_ENTRIES;
  }
}
