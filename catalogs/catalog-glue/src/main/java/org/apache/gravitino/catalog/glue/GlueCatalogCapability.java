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

import java.util.Locale;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

/**
 * Capability declarations for the AWS Glue Data Catalog connector.
 *
 * <p>AWS Glue constraints that deviate from Gravitino defaults:
 *
 * <ul>
 *   <li><b>Case-insensitive names</b>: Glue folds database and table names to lowercase on storage.
 *       See <a
 *       href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html">Database
 *       API</a> and <a
 *       href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html">Table
 *       API</a>: <i>"folded to lowercase when it is stored"</i>.
 *   <li><b>No NOT NULL constraints</b>: The Glue {@code Column} structure has no nullable /
 *       constraint field. See <a
 *       href="https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html">Column API</a>.
 *   <li><b>No DEFAULT values</b>: The Glue {@code Column} structure has no {@code defaultValue}
 *       field. See <a href="https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html">Column
 *       API</a>.
 * </ul>
 */
public class GlueCatalogCapability implements Capability {

  @Override
  public CapabilityResult columnNotNull() {
    // Glue Column structure has no nullable/constraint field — NOT NULL cannot be expressed.
    // See https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html
    return CapabilityResult.unsupported(
        "AWS Glue Data Catalog does not support NOT NULL constraints on columns.");
  }

  @Override
  public CapabilityResult columnDefaultValue() {
    // Glue Column structure has no defaultValue field — DEFAULT cannot be expressed.
    // See https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html
    return CapabilityResult.unsupported(
        "AWS Glue Data Catalog does not support DEFAULT values on columns.");
  }

  @Override
  public CapabilityResult caseSensitiveOnName(Scope scope) {
    switch (scope) {
      case SCHEMA:
      case TABLE:
        // Glue folds database/table names to lowercase on storage.
        // See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html
        // See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html
        return CapabilityResult.unsupported(
            "AWS Glue Data Catalog is case-insensitive for "
                + scope.name().toLowerCase(Locale.ROOT)
                + " names.");
      default:
        return CapabilityResult.SUPPORTED;
    }
  }
}
