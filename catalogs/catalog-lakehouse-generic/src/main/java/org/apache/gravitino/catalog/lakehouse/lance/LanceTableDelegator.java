/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.lance;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.catalog.lakehouse.generic.LakehouseTableDelegator;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.storage.IdGenerator;

public class LanceTableDelegator implements LakehouseTableDelegator {

  public static final String LANCE_TABLE_FORMAT = "lance";

  public static final String PROPERTY_LANCE_TABLE_REGISTER = "lance.register";

  public static final String PROPERTY_LANCE_STORAGE_OPTIONS_PREFIX = "lance.storage.";

  @Override
  public String tableFormat() {
    return LANCE_TABLE_FORMAT;
  }

  @Override
  public List<PropertyEntry<?>> tablePropertyEntries() {
    return ImmutableList.of(
        PropertyEntry.stringOptionalPropertyPrefixEntry(
            PROPERTY_LANCE_STORAGE_OPTIONS_PREFIX,
            "The storage options passed to Lance table.",
            false /* immutable */,
            null /* default value*/,
            false /* hidden */,
            false /* reserved */),
        PropertyEntry.booleanPropertyEntry(
            PROPERTY_LANCE_TABLE_REGISTER,
            "Whether this is a table registration operation.",
            false,
            true /* immutable */,
            false /* defaultValue */,
            false /* hidden */,
            false));
  }

  @Override
  public ManagedTableOperations createTableOps(
      EntityStore store, ManagedSchemaOperations schemaOps, IdGenerator idGenerator) {
    return new LanceTableOperations(store, schemaOps, idGenerator);
  }
}
