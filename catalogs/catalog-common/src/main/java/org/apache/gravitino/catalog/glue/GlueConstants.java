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

import org.apache.gravitino.storage.AWSProperties;

/** Constant keys for the AWS Glue Data Catalog connector configuration and table properties. */
public final class GlueConstants {

  // -------------------------------------------------------------------------
  // Catalog-level connection properties
  // -------------------------------------------------------------------------

  /** AWS region for the Glue Data Catalog (required). */
  public static final String AWS_REGION = "aws-region";

  /**
   * Glue catalog ID — the 12-digit AWS account ID (optional). When omitted, defaults to the
   * caller's AWS account ID.
   */
  public static final String AWS_GLUE_CATALOG_ID = "aws-glue-catalog-id";

  /**
   * AWS access key ID for static credential authentication (optional, hidden). Identifier half of
   * the static credential pair; {@link #AWS_SECRET_ACCESS_KEY} is the secret half.
   */
  public static final String AWS_ACCESS_KEY_ID = AWSProperties.GRAVITINO_AWS_ACCESS_KEY_ID;

  /** AWS secret access key for static credential authentication (optional, sensitive). */
  public static final String AWS_SECRET_ACCESS_KEY = AWSProperties.GRAVITINO_AWS_SECRET_ACCESS_KEY;

  /**
   * Custom Glue endpoint URL (optional). Used for VPC endpoints or LocalStack testing. Example:
   * {@code http://localhost:4566}
   */
  public static final String AWS_GLUE_ENDPOINT = "aws-glue-endpoint";

  /**
   * Upper bound in milliseconds for one Glue API call made by the catalog, including all retries
   * (optional). Defaults to {@link #DEFAULT_AWS_GLUE_API_CALL_TIMEOUT_MS}.
   */
  public static final String AWS_GLUE_API_CALL_TIMEOUT_MS = "aws-glue-api-call-timeout-ms";

  /** Default value for {@link #AWS_GLUE_API_CALL_TIMEOUT_MS}: 30 seconds. */
  public static final long DEFAULT_AWS_GLUE_API_CALL_TIMEOUT_MS = 30_000L;

  /**
   * Upper bound in milliseconds for a single attempt of a Glue API call (optional). Also used as
   * the HTTP connection and socket timeout of the S3 client used for Iceberg tables. Defaults to
   * {@link #DEFAULT_AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS}.
   */
  public static final String AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS =
      "aws-glue-api-call-attempt-timeout-ms";

  /** Default value for {@link #AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS}: 10 seconds. */
  public static final long DEFAULT_AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS = 10_000L;

  /**
   * Maximum number of attempts of a Glue API call made by the catalog, including the first one
   * (optional). Retries apply to throttling and transient failures. Defaults to {@link
   * #DEFAULT_AWS_GLUE_MAX_ATTEMPTS}.
   */
  public static final String AWS_GLUE_MAX_ATTEMPTS = "aws-glue-max-attempts";

  /** Default value for {@link #AWS_GLUE_MAX_ATTEMPTS}. */
  public static final int DEFAULT_AWS_GLUE_MAX_ATTEMPTS = 4;

  /**
   * Default table format used when creating tables via Gravitino's {@code createTable()} API
   * (optional). Accepted values: {@code iceberg}, {@code hive}. Defaults to {@code hive}.
   */
  public static final String DEFAULT_TABLE_FORMAT = "default-table-format";

  /** Default value for {@link #DEFAULT_TABLE_FORMAT}: {@code "hive"}. */
  public static final String DEFAULT_TABLE_FORMAT_VALUE = "hive";

  /**
   * Comma-separated list of table formats exposed by {@code listTables()} and {@code loadTable()}
   * (optional). Defaults to {@code all}.
   */
  public static final String TABLE_FORMAT_FILTER = "table-format-filter";

  /** Default value for {@link #TABLE_FORMAT_FILTER}: expose all table formats. */
  public static final String DEFAULT_TABLE_FORMAT_FILTER = "all";

  /**
   * Base storage path used as a warehouse when no explicit {@code location} is given at table
   * creation time and the Glue database declares no {@code LocationUri}. The table location is
   * derived as {@code warehouse/database/table}.
   *
   * <p>Example warehouse: {@code s3://my-bucket/gravitino-warehouse}.
   */
  public static final String WAREHOUSE = "warehouse";

  // -------------------------------------------------------------------------
  // Glue Table.parameters() keys (passthrough properties)
  // -------------------------------------------------------------------------

  /**
   * Table property that chooses the format of a table at create: {@code HIVE} or {@code ICEBERG}.
   * It is not stored; a loaded table reports it from its {@link #TABLE_TYPE_PARAM} parameter, and
   * it cannot be changed.
   */
  public static final String TABLE_FORMAT = "table-format";

  /** Value of {@link #TABLE_FORMAT} for an Iceberg table. */
  public static final String TABLE_FORMAT_ICEBERG = "ICEBERG";

  /** Value of {@link #TABLE_FORMAT} for a Hive-format table. */
  public static final String TABLE_FORMAT_HIVE = "HIVE";

  /**
   * Table file format (lowercase, e.g. "parquet", "orc", "textfile") used when creating Hive-format
   * tables via Trino. Maps to input-format/output-format/serde-lib.
   */
  public static final String FORMAT = "format";

  /** Iceberg table metadata location stored in Glue {@code Table.parameters()}. */
  public static final String METADATA_LOCATION = "metadata_location";

  /**
   * Key of the {@code Table.parameters()} entry, set to {@code true}, that marks an Iceberg table
   * registered through Gravitino from an existing metadata file. Such a table shares its files with
   * the table it was registered from, so Gravitino changes only its Glue parameters.
   */
  public static final String REGISTERED_ICEBERG_TABLE_PARAM = "gravitino.iceberg.registered";

  /**
   * Key of the {@code table_type} entry in {@code Table.parameters()} that identifies the table
   * format (e.g., {@code "ICEBERG"}).
   */
  public static final String TABLE_TYPE_PARAM = "table_type";

  /** Value of {@link #TABLE_TYPE_PARAM} that identifies an Iceberg table. */
  public static final String ICEBERG_TABLE_TYPE_VALUE = "ICEBERG";

  /** Default Iceberg spec version used when creating Iceberg tables via Glue. */
  public static final String ICEBERG_FORMAT_VERSION = "2";

  /**
   * Key of the {@code current-schema-id} entry in {@code Table.parameters()} that holds the active
   * Iceberg schema ID.
   */
  public static final String CURRENT_SCHEMA_ID_PARAM = "current-schema-id";

  /** Glue column parameter key for the Iceberg field ID (assigned at table creation). */
  public static final String ICEBERG_FIELD_ID = "iceberg.field.id";

  /** Glue column parameter key indicating whether the Iceberg field is optional (nullable). */
  public static final String ICEBERG_FIELD_OPTIONAL = "iceberg.field.optional";

  // -------------------------------------------------------------------------
  // StorageDescriptor-derived table properties (stored in Gravitino properties map)
  // -------------------------------------------------------------------------

  /** Table data location from {@code StorageDescriptor.location()}. */
  public static final String LOCATION = "location";

  /** InputFormat class name from {@code StorageDescriptor.inputFormat()}. */
  public static final String INPUT_FORMAT_CLASS = "input-format";

  /** OutputFormat class name from {@code StorageDescriptor.outputFormat()}. */
  public static final String OUTPUT_FORMAT = "output-format";

  /** SerDe library class name from {@code StorageDescriptor.serDeInfo().serializationLibrary()}. */
  public static final String SERDE_LIB = "serde-lib";

  /** SerDe name from {@code StorageDescriptor.serDeInfo().name()}. */
  public static final String SERDE_NAME = "serde-name";

  /**
   * Prefix for SerDe parameters from {@code StorageDescriptor.serDeInfo().parameters()}. Each SerDe
   * parameter key {@code k} is stored as {@code "serde.parameter." + k}.
   */
  public static final String SERDE_PARAMETER_PREFIX = "serde.parameter.";

  /** Glue {@code tableType} value for external tables. */
  public static final String EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";

  /**
   * Glue {@code tableType} value for views. Both Hive-compatible views (whose definition lives in
   * {@code Table.viewOriginalText()}) and Glue multi-dialect views (whose definition lives in
   * {@code Table.viewDefinition()}) carry this type.
   */
  public static final String VIRTUAL_VIEW_TABLE_TYPE = "VIRTUAL_VIEW";

  private GlueConstants() {}
}
