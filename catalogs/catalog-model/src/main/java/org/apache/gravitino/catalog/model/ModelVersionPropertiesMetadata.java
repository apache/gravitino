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
package org.apache.gravitino.catalog.model;

import static org.apache.gravitino.model.ModelVersion.PROPERTY_DEFAULT_URI_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class ModelVersionPropertiesMetadata extends BasePropertiesMetadata {
  private static final Map<String, PropertyEntry<?>> MODEL_VERSION_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              PROPERTY_DEFAULT_URI_NAME,
              PropertyEntry.stringOptionalPropertyEntry(
                  PROPERTY_DEFAULT_URI_NAME,
                  "The default URI name for the model version",
                  false /* immutable */,
                  null,
                  false /* hidden */))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return MODEL_VERSION_PROPERTY_ENTRIES;
  }
}
