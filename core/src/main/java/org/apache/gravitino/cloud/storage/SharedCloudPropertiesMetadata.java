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

/** Shared cloud-storage credential {@link PropertyEntry} definitions for catalog metadata. */
public final class SharedCloudPropertiesMetadata {

  /** Cloud credential and endpoint keys merged into every catalog's properties metadata. */
  public static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .putAll(S3PropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(OSSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(AzurePropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(GCSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(COSPropertiesMetadata.PROPERTY_ENTRIES)
          .build();

  private SharedCloudPropertiesMetadata() {}
}
