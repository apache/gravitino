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
package org.apache.gravitino.catalog.glue;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class GlueCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  public static final String AWS_REGION = "aws-region";
  public static final String AWS_ACCESS_KEY_ID = "aws-access-key-id";
  public static final String AWS_SECRET_ACCESS_KEY = "aws-secret-access-key";
  public static final String AWS_CATALOG_ID = "aws-catalog-id";

  private static final Map<String, PropertyEntry<?>> GLUE_CATALOG_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              AWS_REGION,
              PropertyEntry.stringRequiredPropertyEntry(
                  AWS_REGION,
                  "The AWS region where the Glue catalog is located",
                  false,
                  false))
          .put(
              AWS_ACCESS_KEY_ID,
              PropertyEntry.stringOptionalPropertyEntry(
                  AWS_ACCESS_KEY_ID,
                  "The AWS access key ID for authenticating with Glue",
                  false,
                  null,
                  false))
          .put(
              AWS_SECRET_ACCESS_KEY,
              PropertyEntry.stringOptionalPropertyEntry(
                  AWS_SECRET_ACCESS_KEY,
                  "The AWS secret access key for authenticating with Glue",
                  false,
                  null,
                  true))
              .put(
                      AWS_CATALOG_ID,
                      PropertyEntry.stringOptionalPropertyEntry(
                              AWS_CATALOG_ID,
                              "The Glue catalog ID to use (e.g. 's3tablescatalog' for S3 Tables). Defaults to the account's default catalog.",
                              false,
                              null,
                              false))

          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return GLUE_CATALOG_PROPERTY_ENTRIES;
  }
}