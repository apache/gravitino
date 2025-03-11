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
package org.apache.gravitino.catalog.lakehouse.hudi;

import static org.apache.gravitino.connector.PropertyEntry.stringImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class HudiTablePropertiesMetadata extends BasePropertiesMetadata {
  public static final String COMMENT = "comment";
  public static final String LOCATION = "location";
  public static final String INPUT_FORMAT = "input-format";
  public static final String OUTPUT_FORMAT = "output-format";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(COMMENT, stringReservedPropertyEntry(COMMENT, "table comment", true /* hidden */))
          .put(
              LOCATION,
              stringImmutablePropertyEntry(
                  LOCATION,
                  "The location for Hudi table",
                  false /* required */,
                  null /* default value */,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              INPUT_FORMAT,
              stringReservedPropertyEntry(
                  INPUT_FORMAT,
                  "Hudi table input format used to distinguish the table type",
                  false /* hidden */))
          .put(
              OUTPUT_FORMAT,
              stringReservedPropertyEntry(
                  OUTPUT_FORMAT, "Hudi table output format", false /* hidden */))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
