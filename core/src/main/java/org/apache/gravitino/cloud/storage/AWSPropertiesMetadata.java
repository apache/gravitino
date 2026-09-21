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
import static org.apache.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.CredentialPropertyUtils;

/**
 * Shared AWS credential {@link PropertyEntry} definitions for catalog properties metadata.
 *
 * <p>{@link #AWS_REGION} is required by Glue and is not part of {@link #PROPERTY_ENTRIES}. Merging
 * it into every catalog would reject catalogs that are not Glue.
 */
public final class AWSPropertiesMetadata {

  /** AWS access key ID. Not hidden. */
  public static final PropertyEntry<String> AWS_ACCESS_KEY_ID =
      stringOptionalPropertyEntry(
          CredentialPropertyUtils.AWS_ACCESS_KEY_ID,
          "AWS access key ID for static credential authentication."
              + " When omitted the default credential chain is used.",
          false /* immutable */,
          null /* defaultValue */,
          false /* hidden */);

  /** AWS secret access key. Hidden. */
  public static final PropertyEntry<String> AWS_SECRET_ACCESS_KEY =
      stringOptionalPropertyEntry(
          CredentialPropertyUtils.AWS_SECRET_ACCESS_KEY,
          "AWS secret access key paired with aws-access-key-id."
              + " When omitted the default credential chain is used.",
          false /* immutable */,
          null /* defaultValue */,
          true /* hidden */);

  /**
   * AWS region for the Glue Data Catalog. Required and immutable. Not merged into every catalog.
   */
  public static final PropertyEntry<String> AWS_REGION =
      stringRequiredPropertyEntry(
          GlueConstants.AWS_REGION,
          "AWS region for the Glue Data Catalog (e.g. us-east-1)",
          true /* immutable */,
          false /* hidden */);

  /** AWS credential keys merged into every catalog's properties metadata. */
  public static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(AWS_ACCESS_KEY_ID.getName(), AWS_ACCESS_KEY_ID)
          .put(AWS_SECRET_ACCESS_KEY.getName(), AWS_SECRET_ACCESS_KEY)
          .build();

  private AWSPropertiesMetadata() {}
}
