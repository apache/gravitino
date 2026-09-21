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
import static org.apache.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import org.apache.gravitino.catalog.glue.GlueConstants;

/**
 * Glue catalog property entries that are not credentials.
 *
 * <p>These stay out of {@link org.apache.gravitino.cloud.storage.SharedCloudPropertiesMetadata}.
 * {@code aws-region} is required, so merging it into every catalog would reject catalogs that are
 * not Glue.
 */
public final class GlueCatalogPropertyEntries {

  /** AWS region for the Glue Data Catalog. Required and immutable. */
  public static final PropertyEntry<String> AWS_REGION =
      stringRequiredPropertyEntry(
          GlueConstants.AWS_REGION,
          "AWS region for the Glue Data Catalog (e.g. us-east-1)",
          true /* immutable */,
          false /* hidden */);

  /** Glue catalog ID. Optional and immutable. */
  public static final PropertyEntry<String> AWS_GLUE_CATALOG_ID =
      stringOptionalPropertyEntry(
          GlueConstants.AWS_GLUE_CATALOG_ID,
          "The 12-digit AWS account ID that owns the Glue catalog."
              + " When omitted, defaults to the caller's AWS account ID.",
          true /* immutable */,
          null /* defaultValue */,
          false /* hidden */);

  /** Custom Glue endpoint URL. Optional and not hidden. */
  public static final PropertyEntry<String> AWS_GLUE_ENDPOINT =
      stringOptionalPropertyEntry(
          GlueConstants.AWS_GLUE_ENDPOINT,
          "Custom Glue endpoint URL for VPC endpoints or LocalStack testing"
              + " (e.g. http://localhost:4566)",
          false /* immutable */,
          null /* defaultValue */,
          false /* hidden */);

  private GlueCatalogPropertyEntries() {}
}
