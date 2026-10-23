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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Combined cloud {@link PropertyEntry} maps so callers do not repeat the same {@code putAll} list.
 */
public final class CloudPropertiesMetadata {

  /**
   * S3, OSS, Azure, GCS, and COS entries. Fileset and schema metadata use this set. It does not
   * include the AWS access-key pair, which is a Glue catalog property.
   */
  public static final Map<String, PropertyEntry<?>> STORAGE_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .putAll(S3PropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(OSSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(AzurePropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(GCSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(COSPropertiesMetadata.PROPERTY_ENTRIES)
          .build();

  /**
   * Storage entries plus the AWS access-key pair. Merged into every catalog. A catalog that already
   * declares a key keeps its own entry.
   */
  public static final Map<String, PropertyEntry<?>> ALL_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .putAll(STORAGE_PROPERTY_ENTRIES)
          .putAll(AWSPropertiesMetadata.PROPERTY_ENTRIES)
          .build();

  private CloudPropertiesMetadata() {}
}
