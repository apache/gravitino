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

/**
 * Re-exports of {@link org.apache.gravitino.catalog.glue.GlueConstants} for use by the Spark
 * connector.
 *
 * <p>The spark-connector module cannot depend on catalog-glue directly, so this class in
 * catalog-common re-exports the constants needed by the Spark side.
 */
public final class GlueConstants {

  // -------------------------------------------------------------------------
  // Catalog-level connection properties
  // -------------------------------------------------------------------------

  /** AWS region for the Glue Data Catalog. */
  public static final String AWS_REGION = "aws-region";

  /** AWS access key ID. */
  public static final String AWS_ACCESS_KEY_ID = "aws-access-key-id";

  /** AWS secret access key. */
  public static final String AWS_SECRET_ACCESS_KEY = "aws-secret-access-key";

  /** Custom Glue endpoint URL for VPC or testing. */
  public static final String AWS_GLUE_ENDPOINT = "aws-glue-endpoint";

  // -------------------------------------------------------------------------
  // Glue table format properties
  // -------------------------------------------------------------------------

  /** Table-format property key for Glue tables. Same as {@code TABLE_FORMAT} in server-side. */
  public static final String TABLE_FORMAT = "table-format";

  /** Table-format value for Iceberg tables stored in Glue. */
  public static final String TABLE_FORMAT_ICEBERG = "ICEBERG";

  private GlueConstants() {}
}
