/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.iceberg.memory;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;

public class MemoryCatalogWithMetadataLocationSupport extends InMemoryCatalog
    implements SupportsMetadataLocation {

  private ConcurrentMap<TableIdentifier, String> tableStore;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    loadFields();
  }

  @Override
  public String metadataLocation(TableIdentifier tableIdentifier) {
    return tableStore.get(tableIdentifier);
  }

  private void loadFields() {
    try {
      this.tableStore =
          (ConcurrentMap<TableIdentifier, String>) FieldUtils.readField(this, "tables", true);
      Preconditions.checkArgument(
          tableStore != null, "Failed to get tables field from memory catalog");
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
