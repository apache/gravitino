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

/** Constant keys for the AWS Glue Data Catalog connector configuration and table properties. */
public final class GlueConstants {

  // -------------------------------------------------------------------------
  // Catalog-level connection properties
  // -------------------------------------------------------------------------

  /** AWS region for the Glue Data Catalog (required). */
  public static final String AWS_REGION = "aws-region";

  /** Glue catalog ID — the 12-digit AWS account ID (required). */
  public static final String AWS_GLUE_CATALOG_ID = "aws-glue-catalog-id";

  /** AWS access key ID for static credential authentication (optional, sensitive). */
  public static final String AWS_ACCESS_KEY_ID = "aws-access-key-id";

  /** AWS secret access key for static credential authentication (optional, sensitive). */
  public static final String AWS_SECRET_ACCESS_KEY = "aws-secret-access-key";

  /**
   * Custom Glue endpoint URL (optional). Used for VPC endpoints or LocalStack testing. Example:
   * {@code http://localhost:4566}
   */
  public static final String AWS_GLUE_ENDPOINT = "aws-glue-endpoint";

  /**
   * Default table format used when creating tables via Gravitino's {@code createTable()} API
   * (optional). Accepted values: {@code iceberg}, {@code hive}. Defaults to {@code hive}.
   */
  public static final String DEFAULT_TABLE_FORMAT = "default-table-format";

  /** Default value for {@link #DEFAULT_TABLE_FORMAT}: {@code "hive"}. */
  public static final String DEFAULT_TABLE_FORMAT_VALUE = "hive";

  /**
   * Comma-separated list of table types exposed by {@code listTables()} and {@code loadTable()}
   * (optional). Accepted values: {@code all}, {@code hive}, {@code iceberg}, {@code delta}, {@code
   * parquet}. Defaults to {@code all}.
   */
  public static final String TABLE_TYPE_FILTER = "table-type-filter";

  /** Default value for {@link #TABLE_TYPE_FILTER}: expose all table types. */
  public static final String TABLE_TYPE_FILTER_ALL = "all";

  // -------------------------------------------------------------------------
  // Glue Table.parameters() keys (passthrough properties)
  // -------------------------------------------------------------------------

  /**
   * Glue table format type parameter key stored in {@code Table.parameters()}. Common values:
   * {@code ICEBERG}, {@code HIVE}, {@code DELTA}, {@code PARQUET}, {@code VIRTUAL_VIEW}.
   */
  public static final String TABLE_FORMAT_TYPE = "table_format_type";

  /** Iceberg table metadata location stored in Glue {@code Table.parameters()}. */
  public static final String METADATA_LOCATION = "metadata_location";

  private GlueConstants() {}
}
