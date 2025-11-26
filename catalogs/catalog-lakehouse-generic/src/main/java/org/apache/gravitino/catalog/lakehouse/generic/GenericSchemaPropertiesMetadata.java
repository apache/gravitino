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
package org.apache.gravitino.catalog.lakehouse.generic;

import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Schema;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class GenericSchemaPropertiesMetadata extends BasePropertiesMetadata {

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringOptionalPropertyEntry(
                Schema.PROPERTY_LOCATION,
                "The root directory of the schema.",
                false /* immutable */,
                null, /* defaultValue */
                false /* hidden */));

    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
