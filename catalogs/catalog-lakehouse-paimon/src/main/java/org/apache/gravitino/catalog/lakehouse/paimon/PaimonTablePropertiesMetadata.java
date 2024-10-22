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
package org.apache.gravitino.catalog.lakehouse.paimon;

import static org.apache.gravitino.connector.PropertyEntry.stringImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Implementation of {@link PropertiesMetadata} that represents Apache Paimon table properties
 * metadata.
 */
public class PaimonTablePropertiesMetadata extends BasePropertiesMetadata {

  public static final String COMMENT = "comment";
  public static final String OWNER = "owner";
  public static final String BUCKET_KEY = "bucket-key";
  public static final String MERGE_ENGINE = "merge-engine";
  public static final String SEQUENCE_FIELD = "sequence.field";
  public static final String ROWKIND_FIELD = "rowkind.field";
  public static final String PRIMARY_KEY = "primary-key";
  public static final String PARTITION = "partition";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(COMMENT, "The table comment", true),
            stringReservedPropertyEntry(OWNER, "The table owner", false),
            stringReservedPropertyEntry(BUCKET_KEY, "The table bucket key", false),
            stringImmutablePropertyEntry(
                MERGE_ENGINE, "The table merge engine", false, null, false, false),
            stringImmutablePropertyEntry(
                SEQUENCE_FIELD, "The table sequence field", false, null, false, false),
            stringImmutablePropertyEntry(
                ROWKIND_FIELD, "The table rowkind field", false, null, false, false),
            stringReservedPropertyEntry(PRIMARY_KEY, "The table primary key", false),
            stringReservedPropertyEntry(PARTITION, "The table partition", false));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
