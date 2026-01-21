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
package org.apache.gravitino.trino.connector.catalog.bigquery;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;

/** Property metadata for BigQuery catalog, table, and schema. */
public class BigQueryPropertyMeta implements HasPropertyMeta {

  // Table property metadata
  public static final List<PropertyMetadata<?>> TABLE_PROPERTY_META = ImmutableList.of();

  // Schema property metadata (BigQuery has no special schema property)
  public static final List<PropertyMetadata<?>> SCHEMA_PROPERTY_META = ImmutableList.of();

  // Column property metadata (BigQuery has no special column property)
  public static final List<PropertyMetadata<?>> COLUMN_PROPERTY_META = ImmutableList.of();

  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }

  public List<PropertyMetadata<?>> getSchemaPropertyMetadata() {
    return SCHEMA_PROPERTY_META;
  }

  public List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return COLUMN_PROPERTY_META;
  }
}
