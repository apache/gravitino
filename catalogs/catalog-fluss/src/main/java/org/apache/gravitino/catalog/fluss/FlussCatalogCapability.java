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

package org.apache.gravitino.catalog.fluss;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.StringUtils;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

/** Capability declarations for the Apache Fluss catalog connector. */
public class FlussCatalogCapability implements Capability {

  private static final Set<String> RESERVED_COLUMN_NAMES =
      ImmutableSet.of(
          TableDescriptor.OFFSET_COLUMN_NAME,
          TableDescriptor.TIMESTAMP_COLUMN_NAME,
          TableDescriptor.BUCKET_COLUMN_NAME,
          TableDescriptor.CHANGE_TYPE_COLUMN,
          TableDescriptor.LOG_OFFSET_COLUMN,
          TableDescriptor.COMMIT_TIMESTAMP_COLUMN);

  /** {@inheritDoc} */
  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    if (scope == Scope.SCHEMA || scope == Scope.TABLE) {
      return validateDatabaseOrTableName(scope, name);
    }

    if (scope == Scope.COLUMN) {
      return validateColumnName(name);
    }

    return Capability.super.specificationOnName(scope, name);
  }

  /** {@inheritDoc} */
  @Override
  public CapabilityResult columnNotNull() {
    return CapabilityResult.SUPPORTED;
  }

  /** {@inheritDoc} */
  @Override
  public CapabilityResult columnDefaultValue() {
    return CapabilityResult.unsupported("Fluss columns do not carry default values.");
  }

  private static CapabilityResult validateDatabaseOrTableName(Scope scope, String name) {
    String invalidName = TablePath.detectInvalidName(name);
    if (invalidName != null) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal in Fluss: %s.", scope, name, invalidName));
    }

    String invalidPrefix = TablePath.validatePrefix(name);
    if (invalidPrefix != null) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal in Fluss: %s.", scope, name, invalidPrefix));
    }

    return CapabilityResult.SUPPORTED;
  }

  private static CapabilityResult validateColumnName(String name) {
    if (StringUtils.isNullOrWhitespaceOnly(name)) {
      return CapabilityResult.unsupported(
          "Fluss column names must contain at least one non-whitespace character.");
    }

    if (RESERVED_COLUMN_NAMES.contains(name)) {
      return CapabilityResult.unsupported(
          String.format("The column name '%s' is reserved as a Fluss system column.", name));
    }

    return CapabilityResult.SUPPORTED;
  }
}
