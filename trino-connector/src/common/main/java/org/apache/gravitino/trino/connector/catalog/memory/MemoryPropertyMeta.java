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

package org.apache.gravitino.trino.connector.catalog.memory;

import static io.trino.spi.session.PropertyMetadata.integerProperty;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;

/**
 * Defines and manages property metadata for memory tables in the Trino connector. This class
 * provides property definitions specific to memory-based tables.
 */
public class MemoryPropertyMeta implements HasPropertyMeta {

  /** List of supported property metadata for memory tables. */
  public static final List<PropertyMetadata<?>> MEMORY_TABLE_PROPERTY =
      ImmutableList.of(integerProperty("max_ttl", "Max ttl days for the table.", 10, false));

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return MEMORY_TABLE_PROPERTY;
  }
}
