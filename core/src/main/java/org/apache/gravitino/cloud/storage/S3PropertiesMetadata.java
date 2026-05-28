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
import org.apache.gravitino.storage.S3Properties;

/** Shared S3 credential {@link PropertyEntry} definitions for catalog properties metadata. */
public class S3PropertiesMetadata {

  public static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
              stringOptionalPropertyEntry(
                  S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
                  "S3 access key ID",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
              stringOptionalPropertyEntry(
                  S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
                  "S3 secret access key",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .build();

  private S3PropertiesMetadata() {}
}
