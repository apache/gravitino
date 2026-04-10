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

import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_ACCESS_KEY_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_CATALOG_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_ENDPOINT;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_REGION;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_FILTER;
import static org.apache.gravitino.catalog.glue.GlueConstants.DEFAULT_TABLE_FORMAT_VALUE;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_FORMAT_FILTER;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Properties metadata for the AWS Glue Data Catalog connector catalog-level configuration. */
public class GlueCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              AWS_REGION,
              stringRequiredPropertyEntry(
                  AWS_REGION,
                  "AWS region for the Glue Data Catalog (e.g. us-east-1)",
                  true /* immutable */,
                  false /* hidden */))
          .put(
              AWS_GLUE_CATALOG_ID,
              stringOptionalPropertyEntry(
                  AWS_GLUE_CATALOG_ID,
                  "The 12-digit AWS account ID that owns the Glue catalog."
                      + " When omitted, defaults to the caller's AWS account ID.",
                  true /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              AWS_ACCESS_KEY_ID,
              stringOptionalPropertyEntry(
                  AWS_ACCESS_KEY_ID,
                  "AWS access key ID for static credential authentication."
                      + " When omitted the default credential chain is used.",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              AWS_SECRET_ACCESS_KEY,
              stringOptionalPropertyEntry(
                  AWS_SECRET_ACCESS_KEY,
                  "AWS secret access key paired with aws-access-key-id."
                      + " When omitted the default credential chain is used.",
                  false /* immutable */,
                  null /* defaultValue */,
                  true /* hidden */))
          .put(
              AWS_GLUE_ENDPOINT,
              stringOptionalPropertyEntry(
                  AWS_GLUE_ENDPOINT,
                  "Custom Glue endpoint URL for VPC endpoints or LocalStack testing"
                      + " (e.g. http://localhost:4566)",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              DEFAULT_TABLE_FORMAT,
              stringOptionalPropertyEntry(
                  DEFAULT_TABLE_FORMAT,
                  "Default format for tables created via createTable(). Accepted: iceberg, hive."
                      + " Unrecognised values are rejected at createTable() time.",
                  false /* immutable */,
                  DEFAULT_TABLE_FORMAT_VALUE,
                  false /* hidden */))
          .put(
              TABLE_FORMAT_FILTER,
              stringOptionalPropertyEntry(
                  TABLE_FORMAT_FILTER,
                  "Comma-separated table formats exposed by listTables() and loadTable().",
                  false /* immutable */,
                  DEFAULT_TABLE_FORMAT_FILTER,
                  false /* hidden */))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
