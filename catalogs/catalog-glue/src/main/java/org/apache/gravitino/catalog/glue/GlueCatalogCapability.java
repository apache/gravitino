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

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

/**
 * Capability declarations for the AWS Glue Data Catalog connector.
 *
 * <p>AWS Glue constraints that deviate from Gravitino defaults:
 *
 * <ul>
 *   <li>Names (database, table, column) are case-insensitive — Glue normalises them to lowercase.
 *   <li>Column NOT NULL constraints are not enforced by Glue.
 *   <li>Column DEFAULT values are not supported by Glue.
 * </ul>
 */
public class GlueCatalogCapability implements Capability {

  @Override
  public CapabilityResult columnNotNull() {
    return CapabilityResult.unsupported(
        "AWS Glue Data Catalog does not enforce NOT NULL constraints on columns.");
  }

  @Override
  public CapabilityResult columnDefaultValue() {
    return CapabilityResult.unsupported(
        "AWS Glue Data Catalog does not support DEFAULT values on columns.");
  }

  @Override
  public CapabilityResult caseSensitiveOnName(Scope scope) {
    switch (scope) {
      case SCHEMA:
      case TABLE:
      case COLUMN:
        // Glue normalises database/table/column names to lowercase.
        return CapabilityResult.unsupported(
            "AWS Glue Data Catalog is case-insensitive for "
                + scope.name().toLowerCase()
                + " names.");
      default:
        return CapabilityResult.SUPPORTED;
    }
  }
}
