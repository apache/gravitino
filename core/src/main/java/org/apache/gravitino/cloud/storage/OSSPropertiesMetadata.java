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
import org.apache.gravitino.storage.OSSProperties;

/** Shared OSS credential {@link PropertyEntry} definitions for catalog properties metadata. */
public class OSSPropertiesMetadata {

  public static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
              stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
                  "OSS access key ID",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
              stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
                  "OSS access key secret",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              OSSProperties.GRAVITINO_OSS_REGION,
              stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_REGION,
                  "OSS service region",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              OSSProperties.GRAVITINO_OSS_ENDPOINT,
              stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_ENDPOINT,
                  "OSS service endpoint",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              OSSProperties.GRAVITINO_OSS_ROLE_ARN,
              stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_ROLE_ARN,
                  "OSS role ARN for STS credential vending",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              OSSProperties.GRAVITINO_OSS_EXTERNAL_ID,
              stringOptionalPropertyEntry(
                  OSSProperties.GRAVITINO_OSS_EXTERNAL_ID,
                  "OSS external ID for cross-account AssumeRole",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .build();

  private OSSPropertiesMetadata() {}
}
